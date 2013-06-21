package com.facebook.presto.split;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.metadata.TablePartition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import javax.inject.Inject;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.metadata.Node.hostAndPortGetter;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.uniqueIndex;

public class NativeSplitManager
        implements ConnectorSplitManager
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";
    private static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");

    private final NodeManager nodeManager;
    private final ShardManager shardManager;
    private final Metadata metadata;

    @Inject
    public NativeSplitManager(NodeManager nodeManager, ShardManager shardManager, Metadata metadata)
    {
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public String getConnectorId()
    {
        return "native";
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        return handle instanceof NativeTableHandle;
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        TableMetadata tableMetadata = metadata.getTableMetadata(table);
        checkState(tableMetadata != null, "no metadata for %s found", table);

        return ImmutableList.copyOf(Collections2.transform(shardManager.getPartitions(table), toPartition(metadata, table, tableMetadata)));
    }

    @Override
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        checkNotNull(partitions, "partitions is null");
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        Map<String, Node> nodesById = uniqueIndex(nodeManager.getActiveNodes(), Node.getIdentifierFunction());

        List<Split> splits = new ArrayList<>();

        for (Partition partition : partitions) {
            checkArgument(partition instanceof NativePartition, "Partition must be a native partition");
            NativePartition nativePartition = (NativePartition) partition;

            Multimap<Long, String> shardNodes = shardManager.getCommittedShardNodesByPartitionId(nativePartition.getNativePartitionId());

            for (Map.Entry<Long, Collection<String>> entry : shardNodes.asMap().entrySet()) {
                List<HostAddress> addresses = getAddressesForNodes(nodesById, entry.getValue());
                Split split = new NativeSplit(entry.getKey(), addresses);
                splits.add(split);
            }
        }

        // the query engine assumes that splits are returned in a somewhat random fashion. The native split manager,
        // because it loads the data from a db table will return the splits somewhat ordered by node id so only a sub
        // set of nodes is fired up. Shuffle the splits to ensure random distribution.
        Collections.shuffle(splits);

        return ImmutableList.copyOf(splits);
    }

    private static List<HostAddress> getAddressesForNodes(Map<String, Node> nodeMap, Iterable<String> nodeIdentifiers)
    {
        return ImmutableList.copyOf(transform(transform(nodeIdentifiers, forMap(nodeMap)), hostAndPortGetter()));
    }

    public static class NativePartition
            implements Partition
    {
        private final long partitionId;
        private Map<ColumnHandle, Object> keys;

        public NativePartition(long partitionId, Map<ColumnHandle, Object> keys)
        {
            this.partitionId = partitionId;
            this.keys = keys;
        }

        @Override
        public String getPartitionId()
        {
            return Long.toString(partitionId);
        }

        public long getNativePartitionId()
        {
            return partitionId;
        }

        @Override
        public Map<ColumnHandle, Object> getKeys()
        {
            return keys;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(partitionId, keys);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final NativePartition other = (NativePartition) obj;
            return this.partitionId == other.partitionId
                    && Objects.equal(this.keys, other.keys);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                    .add("partitionId", partitionId)
                    .add("keys", keys)
                    .toString();
        }
    }

    // similar to HiveClient
    private static Function<TablePartition, Partition> toPartition(final Metadata metadata, final TableHandle tableHandle, TableMetadata tableMetadata)
    {
        ImmutableMap.Builder<String, ColumnMetadata> builder = ImmutableMap.builder();

        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            builder.put(columnMetadata.getName(), columnMetadata);
        }

        final Map<String, ColumnMetadata> columnsByName = builder.build();

        return new Function<TablePartition, Partition>()
        {
            @Override
            public Partition apply(TablePartition tablePartition)
            {
                String partitionName = tablePartition.getPartitionName();

                if (UNPARTITIONED_ID.equals(partitionName)) {
                    return new NativePartition(tablePartition.getPartitionId(), ImmutableMap.<ColumnHandle, Object>of());
                }

                LinkedHashMap<String, String> keys = makeSpecFromName(partitionName);
                ImmutableMap.Builder<ColumnHandle, Object> builder = ImmutableMap.builder();
                for (Map.Entry<String, String> entry : keys.entrySet()) {
                    ColumnMetadata columnMetadata = columnsByName.get(entry.getKey());
                    checkArgument(columnMetadata != null, "Invalid partition key %s in partition %s", entry.getKey(), tablePartition.getPartitionName());
                    ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, entry.getKey()).get();

                    String value = entry.getValue();
                    switch (columnMetadata.getType()) {
                        case BOOLEAN:
                            if (value.length() == 0) {
                                builder.put(columnHandle, false);
                            }
                            else {
                                builder.put(columnHandle, Boolean.parseBoolean(value));
                            }
                            break;
                        case LONG:
                            if (value.length() == 0) {
                                builder.put(columnHandle, 0L);
                            }
                            else {
                                builder.put(columnHandle, Long.parseLong(value));
                            }
                            break;
                        case DOUBLE:
                            if (value.length() == 0) {
                                builder.put(columnHandle, 0L);
                            }
                            else {
                                builder.put(columnHandle, Double.parseDouble(value));
                            }
                            break;
                        case STRING:
                            builder.put(columnHandle, value);
                            break;
                    }
                }

                return new NativePartition(tablePartition.getPartitionId(), builder.build());
            }
        };
    }

    // similar to Warehouse.makeSpecFromName
    private static LinkedHashMap<String, String> makeSpecFromName(String name)
    {
        checkArgument(name != null, "name is null");
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
        File path = new File(name);
        List<String[]> kvs = new ArrayList<String[]>();
        do {
            String component = path.getName();
            Matcher m = pat.matcher(component);
            if (m.matches()) {
                String[] kv = new String[2];
                kv[0] = unescapePathName(m.group(1));
                kv[1] = unescapePathName(m.group(2));
                kvs.add(kv);
            }
            path = path.getParentFile();
        }
        while (path != null && !path.getName().isEmpty());

        // reverse the list since we checked the part from leaf dir to table's base dir
        for (int i = kvs.size(); i > 0; i--) {
            partSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
        }

        return partSpec;
    }

    // similar to FileUtils.unescapePathName
    private static String unescapePathName(String path)
    {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;
                try {
                    code = Integer.valueOf(path.substring(i + 1, i + 3), 16);
                }
                catch (Exception e) {
                    code = -1;
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
