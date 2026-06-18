ORC File
========

Structure
---------

    stripe_0
    stripe_1
    stripe_2
    ...
    stripe_N
    metadata
    footer
    postScript
    postScriptSize

Decoding
--------

The file is decoded bottom up:

1. Read `postScriptSize` from last byte of the file
2. Read post script section
3. Decode `PostScript` using Protocol Buffers
4. Read metadata and footer sections using information from postScript
5. Decode `Metadata` and `Footer` using Protocol Buffers
6. Filter stripes using `Metadata` (stop if everything filtered)
7. Read stripes using `StripeInformation` from Footer
8. Decode stripes (see below)

Stripe
======

Structure
---------

    index
        index_stream_0
        index_stream_1
        index_stream_2
        ...
        index_stream_3
    data
        data_stream_0
        data_stream_1
        data_stream_2
        ...
        data_stream_3
    stripeFooter

Decoding
--------

A stripe is decoded as follows:

1. Read stripeFooter
2. Decode StripeFooter using Protocol Buffers
3. Read index streams for included columns
4. Decode a `List<RowGroupIndex>` from each index_stream using Protocol Buffers
5. Filter row groups using `RowGroupIndex.columnStatistics` (stop if everything filtered)
6. Read data streams for included columns
7. Decode data streams (see below)

Stream
======

Streams contain a sequence of all values for the stripe.  Since a stripe typically contains
hundreds of thousands of values in a compressed stream, the offset of every Nth value is
recorded in the index_stream along with the minimum and maximum value of the stream.  These
N rows logically form a RowGroup, and using the statistics RowGroups can be skipped.
