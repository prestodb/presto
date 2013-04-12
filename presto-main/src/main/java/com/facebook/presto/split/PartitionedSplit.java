package com.facebook.presto.split;

public interface PartitionedSplit
    extends Split
{
    String getPartition();

    boolean isLastSplit();
}
