package com.facebook.presto.ingest;

import com.facebook.presto.block.uncompressed.UncompressedBlock;

import java.util.Iterator;

public interface BlockExtractor
{
    Iterator<UncompressedBlock> extract(Readable source);
}
