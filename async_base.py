# Copyright (c) 2020 University Corporation for Atmospheric Research/Unidata.
# Distributed under the terms of the MIT License.
# SPDX-License-Identifier: MIT
import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import logging
import sys

logger = logging.getLogger('alchemy.async')

class Main:
    """Sets up main processing for async processing pipeline."""
    def __init__(self, *, nthreads=20):
        self.nthreads = nthreads
        self.sinks = []
        self.tasks = []

    def add_standalone_task(self, async_func):
        task = asyncio.ensure_future(async_func())
        self.tasks.append(task)

    def connect(self, dest):
        task = asyncio.ensure_future(dest.mainloop())
        self.tasks.append(task)
        self.sinks.append(dest.queue)

    def run(self):
        try:
            # Get the default loop and add an executor for running synchronous code
            self.loop = asyncio.get_event_loop()
            self.loop.set_default_executor(ThreadPoolExecutor(self.nthreads))

            # Add a debugging task that shows the current task count
            self.add_standalone_task(self.log_tasks)

            # Run our main task until it finishes
            logger.info('Starting main event loop.')
            self.loop.run_until_complete(self.mainloop())

            # Close up
            self.loop.close()
        except Exception as e:
            logger.exception('Exception raised!', exc_info=e)

    async def mainloop(self):
        # Continuously loop, reading products and sending them to connected tasks
        async for item in self:
            for sink in self.sinks:
                await sink.put(item)

        logger.warning('Done reading items.')
        for sink in self.sinks:
            logger.debug('Flushing sink: %s', sink)
            await sink.join()

        for t in self.tasks:
            t.cancel()
        await asyncio.sleep(0.05)  # Just enough to let other things close out

    async def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def log_tasks(self):
        while True:
            await asyncio.sleep(60)
            logger.info('Tasks: %d', len(asyncio.Task.all_tasks(self.loop)))


class Job:
    def __init__(self, name):
        self.name = name
        self.queue = asyncio.Queue()
        self.loop = asyncio.get_event_loop()

    async def mainloop(self):
        while True:
            item = await self.queue.get()
            try:
                fut = self.loop.run_in_executor(None, self.run, item)
                fut.add_done_callback(functools.partial(self.done_callback, item))
            except Exception:
                logger.exception(f'Exception executing task {self.name}:',
                                 exc_info=sys.exc_info())

    def done_callback(self, item, future):
        try:
            res = future.result()
            self.finish(item, res)
        except IOError:
            logger.warning('Failed to process %s. Queuing for retry...', item)
            self.loop.call_later(15, self.queue.put_nowait, item)
        except Exception:
            logger.exception('Exception on finishing item %s:', item, exc_info=sys.exc_info())
        finally:
            self.queue.task_done()

    def run(self, item):
        logger.debug('Processing %s...', item)

    def finish(self, item, result):
        logger.debug('Finished %s.', item)
