package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.spi.WarningCollector;

public interface HivePartitionVerifier
{
    void verify(Partition partition, WarningCollector warningCollector);

    void aggregateWarning(WarningCollector warningCollector);
}
