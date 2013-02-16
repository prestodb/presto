package com.facebook.presto.hive;

import org.apache.hadoop.hive.metastore.api.Partition;

class UnpartitionedPartition
        extends Partition
{
    static final String UNPARTITIONED_NAME = "<UNPARTITIONED>";
    static final UnpartitionedPartition INSTANCE = new UnpartitionedPartition();
}
