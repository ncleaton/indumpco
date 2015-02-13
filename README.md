# indumpco
Incremental Dump Compressor

A compressor for a repeated textual dump of something, such as a
daily pg\_dump backup of postgresql database.

The dump is split into segments for compression, and compressed
segments from previous days' dumps can be reused for sections of
the dump that have not changed.

Segment sizes are not fixed, instead segment boundaries are
placed according to patterns in the input.  This prevents a change
near the start of the dump from pushing all subsequent segments
out of alignment and spoiling the prospects for segment reuse.

See bin/incremental-dump-example for an example.
