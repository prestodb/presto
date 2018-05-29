/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CORRUPTED_MAPPING_METADATA;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RowType.Field;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);

    private final ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("elasticsearch-metadata-%s"));
    private final ObjectMapper objecMapper = new ObjectMapperProvider().get();
    private final ElasticsearchTableDescriptionProvider tableDescriptions;
    private final Map<String, TransportClient> clients = new HashMap<>();
    private final LoadingCache<ElasticsearchTableDescription, List<ColumnMetadata>> tableColumnsCache;
    private final Duration timeout;

    @Inject
    public ElasticsearchClient(ElasticsearchTableDescriptionProvider descriptions, Duration requestTimeout)
            throws IOException
    {
        tableDescriptions = requireNonNull(descriptions, "description is null");
        timeout = requireNonNull(requestTimeout, "requestTimeout is null");

        for (ElasticsearchTableDescription tableDescription : tableDescriptions.getAllTableDescriptions()) {
            if (!clients.containsKey(tableDescription.getClusterName())) {
                Settings settings = Settings.builder().put("cluster.name", tableDescription.getClusterName()).build();
                TransportAddress address = new TransportAddress(InetAddress.getByName(tableDescription.getHost()), tableDescription.getPort());
                TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(address);
                clients.put(tableDescription.getClusterName(), client);
            }
        }
        this.tableColumnsCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, MINUTES)
                .refreshAfterWrite(15, MINUTES)
                .maximumSize(500)
                .build(asyncReloading(CacheLoader.from(this::loadColumns), executor));
    }

    @PreDestroy
    public void tearDown()
    {
        try (Closer closer = Closer.create()) {
            for (Entry<String, TransportClient> entry : clients.entrySet()) {
                closer.register(entry.getValue());
            }
            closer.register(clients::clear);
            closer.register(() -> executor.shutdown());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> listSchemas()
    {
        return tableDescriptions.getAllSchemaTableNames().stream()
                    .map(SchemaTableName::getSchemaName)
                    .collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .filter(schemaTableName -> !schemaName.isPresent() || schemaTableName.getSchemaName().equals(schemaName.get()))
                .collect(toImmutableList());
    }

    private List<ColumnMetadata> loadColumns(ElasticsearchTableDescription table)
    {
        if (table.getColumns().isPresent()) {
            return buildMetadata(table.getColumns().get());
        }
        List<ElasticsearchColumn> columns = buildColumns(table);
        return buildMetadata(columns);
    }

    public List<ColumnMetadata> getColumnMetadata(ElasticsearchTableDescription tableDescription)
    {
        return tableColumnsCache.getUnchecked(tableDescription);
    }

    public ElasticsearchTableDescription getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        ElasticsearchTableDescription table = tableDescriptions.get(new SchemaTableName(schemaName, tableName));
        if (table == null) {
            return null;
        }
        if (table.getColumns().isPresent()) {
            return table;
        }
        List<ElasticsearchColumn> columns = buildColumns(table);
        ElasticsearchTableDescription result = new ElasticsearchTableDescription(
                table.getTableName(),
                table.getSchemaName(),
                table.getHost(),
                table.getPort(),
                table.getClusterName(),
                table.getIndex(),
                table.getIndexExactMatch(),
                table.getType(),
                Optional.of(columns));
        return result;
    }

    public List<String> getIndices(ElasticsearchTableDescription tableDescription)
    {
        if (tableDescription.getIndexExactMatch()) {
            return ImmutableList.of(tableDescription.getIndex());
        }
        TransportClient client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client is null");
        String[] indices = client.admin()
                .indices()
                .getIndex(new GetIndexRequest())
                .actionGet(timeout.toMillis())
                .getIndices();
        return Arrays.stream(indices)
                    .filter(index -> index.startsWith(tableDescription.getIndex()))
                    .collect(toImmutableList());
    }

    public ClusterSearchShardsResponse getSearchShards(String index, ElasticsearchTableDescription tableDescription)
    {
        TransportClient client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client is null");
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest(index);
        return client.admin()
                .cluster()
                .searchShards(request)
                .actionGet(timeout.toMillis());
    }

    private List<ColumnMetadata> buildMetadata(List<ElasticsearchColumn> columns)
    {
        List<ColumnMetadata> result = new ArrayList<>();
        for (ElasticsearchColumn column : columns) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("jsonPath", column.getJsonPath());
            properties.put("jsonType", column.getJsonType());
            properties.put("isList", column.isList());
            properties.put("ordinalPosition", column.getOrdinalPosition());
            result.add(new ColumnMetadata(column.getName(), column.getType(), "", "", false, properties));
        }
        return result;
    }

    private List<ElasticsearchColumn> buildColumns(ElasticsearchTableDescription tableDescription)
    {
        List<ElasticsearchColumn> columns = new ArrayList<>();
        TransportClient client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client is null");
        for (String index : getIndices(tableDescription)) {
            try {
                GetMappingsRequest mappingsRequest = new GetMappingsRequest().types(tableDescription.getType());

                if (!isNullOrEmpty(index)) {
                    mappingsRequest.indices(index);
                }
                ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = client.admin()
                        .indices()
                        .getMappings(mappingsRequest)
                        .actionGet(timeout.toMillis())
                        .getMappings();

                Iterator<String> indexIterator = mappings.keysIt();
                while (indexIterator.hasNext()) {
                    // TODO use io.airlift.json.JsonCodec
                    MappingMetaData mappingMetaData = mappings.get(indexIterator.next()).get(tableDescription.getType());
                    JsonNode rootNode = objecMapper.readTree(mappingMetaData.source().uncompressed());
                    JsonNode mappingNode = rootNode.get(tableDescription.getType());
                    JsonNode propertiesNode = mappingNode.get("properties");

                    List<String> lists = new ArrayList<>();
                    JsonNode metaNode = mappingNode.get("_meta");
                    if (metaNode != null) {
                        JsonNode arrayNode = metaNode.get("lists");
                        if (arrayNode != null && arrayNode.isArray()) {
                            ArrayNode arrays = (ArrayNode) arrayNode;
                            for (int i = 0; i < arrays.size(); i++) {
                                lists.add(arrays.get(i).textValue());
                            }
                        }
                    }
                    populateColumns(propertiesNode, lists, columns);
                }
            }
            catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_CORRUPTED_MAPPING_METADATA, e);
            }
        }
        return columns;
    }

    private List<String> getColumnMetadata(String parent, JsonNode propertiesNode)
    {
        List<String> metadata = new ArrayList<>();
        Iterator<Entry<String, JsonNode>> iterator = propertiesNode.fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> entry = iterator.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            String childKey;
            if (parent == null || parent.isEmpty()) {
                childKey = key;
            }
            else {
                childKey = parent.concat(".").concat(key);
            }

            if (value.isObject()) {
                metadata.addAll(getColumnMetadata(childKey, value));
                continue;
            }

            if (!value.isArray()) {
                metadata.add(childKey.concat(":").concat(value.textValue()));
            }
        }
        return metadata;
    }

    private void populateColumns(JsonNode propertiesNode, List<String> arrays, List<ElasticsearchColumn> columns)
    {
        Comparator<String> stringComparator = (key1, key2) -> {
            int length1 = key1.split("\\.").length;
            int length2 = key2.split("\\.").length;
            if (length1 == length2) {
                return key1.compareTo(key2);
            }
            return length2 - length1;
        };
        TreeMap<String, Type> fieldsMap = new TreeMap<>(stringComparator);
        for (String columnMetadata : getColumnMetadata(null, propertiesNode)) {
            int delimiterIndex = columnMetadata.lastIndexOf(":");
            if (delimiterIndex == -1 || delimiterIndex == columnMetadata.length() - 1) {
                LOG.debug("Invalid column path format: %s", columnMetadata);
                continue;
            }
            String fieldName = columnMetadata.substring(0, delimiterIndex);
            String typeName = columnMetadata.substring(delimiterIndex + 1);

            if (!fieldName.endsWith(".type")) {
                LOG.debug("Ignoring column with no type info: %s", columnMetadata);
                continue;
            }
            String propertyName = fieldName.substring(0, fieldName.lastIndexOf('.'));
            String nestedName = propertyName.replaceAll("properties\\.", "");
            if (nestedName.contains(".")) {
                fieldsMap.put(nestedName, getPrestoType(typeName));
            }
            else {
                boolean newNestedColumn = columns.stream()
                        .noneMatch(column -> column.getName().equalsIgnoreCase(nestedName));
                if (newNestedColumn) {
                    columns.add(new ElasticsearchColumn(nestedName, getPrestoType(typeName), nestedName, typeName, arrays.contains(nestedName), -1));
                }
            }
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private void processNestedFields(TreeMap<String, Type> fieldsMap, List<ElasticsearchColumn> columns, List<String> arrays)
    {
        if (fieldsMap.size() == 0) {
            return;
        }
        Entry<String, Type> first = fieldsMap.firstEntry();
        String field = first.getKey();
        Type type = first.getValue();
        if (field.contains(".")) {
            String prefix = field.substring(0, field.lastIndexOf('.'));
            ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            int size = field.split("\\.").length;
            Iterator<String> iterator = fieldsMap.navigableKeySet().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next();
                if (name.split("\\.").length == size && name.startsWith(prefix)) {
                    Optional<String> columnName = Optional.of(name.substring(name.lastIndexOf('.') + 1));
                    Type columnType = fieldsMap.get(name);
                    Field column = new Field(columnName, columnType);
                    fieldsBuilder.add(column);
                    iterator.remove();
                }
            }
            fieldsMap.put(prefix, RowType.from(fieldsBuilder.build()));
        }
        else {
            boolean isFlattenColumn = columns.stream()
                    .noneMatch(column -> column.getName().equalsIgnoreCase(field));
            if (isFlattenColumn) {
                columns.add(new ElasticsearchColumn(field, type, field, type.getDisplayName(), arrays.contains(field), -1));
            }
            fieldsMap.remove(field);
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private static Type getPrestoType(String elasticsearchType)
    {
        switch (elasticsearchType) {
            case "double":
            case "float":
                return DOUBLE;
            case "integer":
                return INTEGER;
            case "long":
                return BIGINT;
            case "string":
            case "text":
            case "keyword":
                return VARCHAR;
            case "boolean":
                return BOOLEAN;
            case "binary":
                return VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported type: " + elasticsearchType);
        }
    }
}
