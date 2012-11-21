package com.facebook.presto.split;

import com.facebook.presto.ingest.SerializedPartitionChunk;
import com.facebook.presto.metadata.ImportTableHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.IterableUtils.limit;
import static com.facebook.presto.util.IterableUtils.shuffle;
import static com.facebook.presto.util.RetryDriver.runWithRetryUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;

public class SplitManager
{
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final ImportClientFactory importClientFactory;

    @Inject
    public SplitManager(NodeManager nodeManager, ShardManager shardManager, ImportClientFactory importClientFactory)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
    }

    public Iterable<SplitAssignments> getSplitAssignments(TableHandle handle)
    {
        switch (handle.getDataSourceType()) {
            case NATIVE:
                return getNativeSplitAssignments((NativeTableHandle) handle);
            case IMPORT:
                return getImportSplitAssignments((ImportTableHandle) handle);
        }
        throw new IllegalArgumentException("unsupported handle type: " + handle);
    }

    private Iterable<SplitAssignments> getNativeSplitAssignments(NativeTableHandle handle)
    {
        Map<String, Node> nodeMap = getNodeMap(nodeManager.getActiveNodes());
        Multimap<Long, String> shardNodes = shardManager.getShardNodes(handle.getTableId());

        ImmutableList.Builder<SplitAssignments> splitAssignments = ImmutableList.builder();
        for (Map.Entry<Long, Collection<String>> entry : shardNodes.asMap().entrySet()) {
            Split split = new NativeSplit(entry.getKey());
            List<Node> nodes = getNodes(nodeMap, entry.getValue());
            splitAssignments.add(new SplitAssignments(split, nodes));
        }
        return splitAssignments.build();
    }

    private static List<Node> getNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        return ImmutableList.copyOf(Iterables.transform(nodeIdentifiers, Functions.forMap(nodeMap)));
    }

    private static Map<String, Node> getNodeMap(Set<Node> nodes)
    {
        return Maps.uniqueIndex(nodes, Node.getIdentifierFunction());
    }

    private Iterable<SplitAssignments> getImportSplitAssignments(ImportTableHandle handle)
    {
        final String sourceName = handle.getSourceName();
        final String databaseName = handle.getDatabaseName();
        final String tableName = handle.getTableName();

        final List<String> partitions = runWithRetryUnchecked(new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                return importClient.getPartitionNames(databaseName, tableName);
            }
        });

        Iterable<List<PartitionChunk>> chunks = runWithRetryUnchecked(new Callable<Iterable<List<PartitionChunk>>>()
        {
            @Override
            public Iterable<List<PartitionChunk>> call()
                    throws Exception
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                return importClient.getPartitionChunks(databaseName, tableName, partitions);
            }
        });

        return Iterables.transform(Iterables.concat(chunks), createImportSplitFunction(sourceName));
    }

    private Function<PartitionChunk, SplitAssignments> createImportSplitFunction(final String sourceName)
    {
        return new Function<PartitionChunk, SplitAssignments>()
        {
            @Override
            public SplitAssignments apply(PartitionChunk chunk)
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                Split split = new ImportSplit(sourceName, SerializedPartitionChunk.create(importClient, chunk));
                List<Node> nodes = limit(shuffle(nodeManager.getActiveNodes()), 3);
                return new SplitAssignments(split, nodes);
            }
        };
    }
}
