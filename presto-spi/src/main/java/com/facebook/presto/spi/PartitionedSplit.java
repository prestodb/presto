package com.facebook.presto.spi;

public interface PartitionedSplit
        extends Split
{
    String getPartitionId();

    boolean isLastSplit();
}
