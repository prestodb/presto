package com.facebook.presto.hive;

import org.apache.hadoop.hive.metastore.api.Partition;

class UnpartitionedPartition
        extends Partition
{
    static final Partition UNPARTITIONED_PARTITION = new UnpartitionedPartition();

    private UnpartitionedPartition()
    {
    }
}
