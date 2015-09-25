# Python LDM feed handler
Python code to facilitate on-the-fly processing of products coming from an LDM feed.

Currently, this project is only used to handle the NEXRAD Level 2 data feed coming across the LDM data feed,
with options to dump the raw data or repackage as a bz2 or gz compressed file. There are also options to upload to
Amazon S3.

The core of this is based around Python 3.5's new `async` support, using the `asyncio` library. As such, only
Python 3.5 will ever work here. One of the core features, though, is that a single python process is able to
handle and process (using multiple threads) the full level 2 stream.

There are no supported APIs here yet, as right now this is just a monotlithic script. Eventually, this same
infrastructure will be used for other LDM data feeds, at which point useful API separation/abstraction will
present itself.
