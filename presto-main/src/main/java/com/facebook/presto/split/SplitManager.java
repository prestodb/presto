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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class SplitManager
{
    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final ImportClient importClient;

    public SplitManager(NodeManager nodeManager, ShardManager shardManager, ImportClient importClient)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.importClient = checkNotNull(importClient, "importClient is null");
    }

    public Iterable<SplitNodes> getSplitNodes(TableHandle handle)
    {
        switch (handle.getDataSourceType()) {
            case NATIVE:
                return getNativeSplitNodes((NativeTableHandle) handle);
            case IMPORT:
                return getImportSplitNodes((ImportTableHandle) handle);
        }
        throw new IllegalArgumentException("unsupported handle type: " + handle);
    }

    private Iterable<SplitNodes> getNativeSplitNodes(NativeTableHandle handle)
    {
        Map<String, Node> nodeMap = getNodeMap(nodeManager.getActiveNodes());
        Multimap<Long, String> shardNodes = shardManager.getShardNodes(handle.getTableId());

        ImmutableList.Builder<SplitNodes> splitNodes = ImmutableList.builder();
        for (Map.Entry<Long, Collection<String>> entry : shardNodes.asMap().entrySet()) {
            Split split = new NativeSplit(entry.getKey());
            List<Node> nodes = getNodes(nodeMap, entry.getValue());
            splitNodes.add(new SplitNodes(split, nodes));
        }
        return splitNodes.build();
    }

    private static List<Node> getNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        for (String nodeIdentifier : nodeIdentifiers) {
            Node node = nodeMap.get(nodeIdentifier);
            if (node == null) {
                throw new RuntimeException("node not available: " + nodeIdentifier);
            }
            nodes.add(node);
        }
        return nodes.build();
    }

    private static Map<String, Node> getNodeMap(Set<Node> nodes)
    {
        ImmutableMap.Builder<String, Node> map = ImmutableMap.builder();
        for (Node node : nodes) {
            map.put(node.getNodeIdentifier(), node);
        }
        return map.build();
    }

    private Iterable<SplitNodes> getImportSplitNodes(ImportTableHandle handle)
    {
        String sourceName = handle.getSourceName();
        String databaseName = handle.getDatabaseName();
        String tableName = handle.getTableName();

        checkArgument(sourceName.equals("hive"), "unsupported source name");

        List<String> partitions = importClient.getPartitionNames(databaseName, tableName);
        Iterable<List<PartitionChunk>> chunks = importClient.getPartitionChunks(databaseName, tableName, partitions);
        return Iterables.transform(Iterables.concat(chunks), createImportSplitFunction(sourceName));
    }

    private Function<PartitionChunk, SplitNodes> createImportSplitFunction(final String sourceName)
    {
        return new Function<PartitionChunk, SplitNodes>()
        {
            @Override
            public SplitNodes apply(PartitionChunk chunk)
            {
                Split split = new ImportSplit(sourceName, SerializedPartitionChunk.create(importClient, chunk));
                List<Node> nodes = limit(shuffle(nodeManager.getActiveNodes()), 3);
                return new SplitNodes(split, nodes);
            }
        };
    }

    private static <T> List<T> limit(Iterable<T> iterable, int limitSize)
    {
        return ImmutableList.copyOf(Iterables.limit(iterable, limitSize));
    }

    private static <T> List<T> shuffle(Iterable<T> iterable)
    {
        List<T> list = Lists.newArrayList(iterable);
        Collections.shuffle(list);
        return list;
    }
}
