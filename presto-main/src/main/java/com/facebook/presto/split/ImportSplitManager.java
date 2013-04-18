package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportSplitManager
        implements ConnectorSplitManager
{
    private final String dataSourceName;
    private final ImportClient importClient;

    public ImportSplitManager(String dataSourceName, ImportClient importClient)
    {
        this.dataSourceName = checkNotNull(dataSourceName, "dataSourceName is null");
        this.importClient = checkNotNull(importClient, "importClient is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return importClient.canHandle(tableHandle);
    }

    @Override
    public List<Partition> getPartitions(final TableHandle tableHandle, final Map<ColumnHandle, Object> bindings)
    {
        return retry()
                .stopOn(NotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<Partition>>()
                {
                    @Override
                    public List<Partition> call()
                            throws Exception
                    {
                        // todo remap handles
                        return importClient.getPartitions(tableHandle, bindings);
                    }
                });
    }

    @Override
    public DataSource getPartitionSplits(final List<Partition> partitions)
    {
        Iterable<Split> splits = retry()
                .stopOn(NotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<Iterable<Split>>()
                {
                    @Override
                    public Iterable<Split> call()
                            throws Exception
                    {
                        // todo remap partitions
                        return importClient.getPartitionSplits(partitions);
                    }
                });
        return new DataSource(dataSourceName, splits);
    }
}
