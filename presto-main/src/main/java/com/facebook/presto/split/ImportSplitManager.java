package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.HostAddress;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.ImportTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

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
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof ImportTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        checkArgument(table instanceof ImportTableHandle, "Table is not an import table %s", table);
        ImportTableHandle importTableHandle = (ImportTableHandle) table;
        final TableHandle clientTableHandle = importTableHandle.getTableHandle();

        ImmutableMap.Builder<ColumnHandle,Object> builder = ImmutableMap.builder();
        for (Entry<ColumnHandle, Object> entry : bindings.entrySet()) {
            ColumnHandle columnHandle = ((ImportColumnHandle) entry.getKey()).getColumnHandle();
            builder.put(columnHandle, entry.getValue());
        }
        final ImmutableMap<ColumnHandle, Object> columnBindings = builder.build();

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
                        return importClient.getPartitions(clientTableHandle, columnBindings);
                    }
                });
    }

    @Override
    public DataSource getPartitionSplits(final List<Partition> partitions, final List<ColumnHandle> columns)
    {
        Iterable<PartitionChunk> partitionChunks = retry()
                .stopOn(NotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<Iterable<PartitionChunk>>()
                {
                    @Override
                    public Iterable<PartitionChunk> call()
                            throws Exception
                    {
                        // todo remap partitions
                        return importClient.getPartitionChunks(partitions, columns);
                    }
                });
        return new DataSource(dataSourceName, transform(partitionChunks, new Function<PartitionChunk, Split>()
        {
            @Override
            public Split apply(PartitionChunk chunk)
            {
                return new ImportSplit(dataSourceName,
                        chunk.getPartitionName(),
                        chunk.isLastChunk(),
                        SerializedPartitionChunk.create(importClient, chunk),
                        toAddresses(chunk.getHosts()),
                        chunk.getInfo());
            }
        }));
    }

    private List<HostAddress> toAddresses(List<InetAddress> inetAddresses)
    {
        return ImmutableList.copyOf(transform(inetAddresses, new Function<InetAddress, HostAddress>()
        {
            @Override
            public HostAddress apply(InetAddress input)
            {
                return HostAddress.fromString(input.getHostAddress());
            }
        }));
    }
}
