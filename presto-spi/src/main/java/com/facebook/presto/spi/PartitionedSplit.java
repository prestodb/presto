package com.facebook.presto.spi;

import java.util.List;

public interface PartitionedSplit
        extends Split
{
    String getPartitionId();

    boolean isLastSplit();

    List<? extends PartitionKey> getPartitionKeys();
}
