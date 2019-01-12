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
package io.prestosql.plugin.kudu.properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KeyEncoderAccessor;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.shaded.com.google.common.base.Predicates;
import org.apache.kudu.shaded.com.google.common.collect.Iterators;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.inject.Inject;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class KuduTableProperties
{
    public static final String PARTITION_BY_HASH_COLUMNS = "partition_by_hash_columns";
    public static final String PARTITION_BY_HASH_BUCKETS = "partition_by_hash_buckets";
    public static final String PARTITION_BY_HASH_COLUMNS_2 = "partition_by_second_hash_columns";
    public static final String PARTITION_BY_HASH_BUCKETS_2 = "partition_by_second_hash_buckets";
    public static final String PARTITION_BY_RANGE_COLUMNS = "partition_by_range_columns";
    public static final String RANGE_PARTITIONS = "range_partitions";
    public static final String NUM_REPLICAS = "number_of_replicas";
    public static final String PRIMARY_KEY = "primary_key";
    public static final String NULLABLE = "nullable";
    public static final String ENCODING = "encoding";
    public static final String COMPRESSION = "compression";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final long DEFAULT_DEADLINE = 20000; // deadline for retrieving range partitions in milliseconds

    private final List<PropertyMetadata<?>> tableProperties;

    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public KuduTableProperties(TypeManager typeManager)
    {
        tableProperties = ImmutableList.of(
                new PropertyMetadata<>(
                        PARTITION_BY_HASH_COLUMNS,
                        "Columns for optional first hash partition level",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                integerProperty(
                        PARTITION_BY_HASH_BUCKETS,
                        "Number of buckets for optional first hash partition level.",
                        null,
                        false),
                new PropertyMetadata<>(
                        PARTITION_BY_HASH_COLUMNS_2,
                        "Columns for optional second hash partition level",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                integerProperty(
                        PARTITION_BY_HASH_BUCKETS_2,
                        "Number of buckets for optional second hash partition level.",
                        null,
                        false),
                new PropertyMetadata<>(
                        PARTITION_BY_RANGE_COLUMNS,
                        "Columns for optional range partition level",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                integerProperty(
                        NUM_REPLICAS,
                        "Number of tablet replicas. Uses default value from Kudu master if not specified.",
                        null,
                        false),
                stringProperty(
                        RANGE_PARTITIONS,
                        "Initial range partitions as JSON",
                        null,
                        false));

        columnProperties = ImmutableList.of(
                booleanProperty(
                        PRIMARY_KEY,
                        "If column belongs to primary key",
                        false,
                        false),
                booleanProperty(
                        NULLABLE,
                        "If column can be set to null",
                        false,
                        false),
                stringProperty(
                        ENCODING,
                        "Optional specification of the column encoding. Otherwise default encoding is applied.",
                        null,
                        false),
                stringProperty(
                        COMPRESSION,
                        "Optional specification of the column compression. Otherwise default compression is applied.",
                        null,
                        false));
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    public static PartitionDesign getPartitionDesign(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        List<String> hashColumns = (List) tableProperties.get(PARTITION_BY_HASH_COLUMNS);
        List<String> hashColumns2 = (List) tableProperties.get(PARTITION_BY_HASH_COLUMNS_2);

        PartitionDesign design = new PartitionDesign();
        if (!hashColumns.isEmpty()) {
            List<HashPartitionDefinition> hashPartitions = new ArrayList<>();
            HashPartitionDefinition hash1 = getHashPartitionDefinition(tableProperties, hashColumns, PARTITION_BY_HASH_BUCKETS);
            hashPartitions.add(hash1);
            if (!hashColumns2.isEmpty()) {
                HashPartitionDefinition hash2 = getHashPartitionDefinition(tableProperties, hashColumns2, PARTITION_BY_HASH_BUCKETS_2);
                hashPartitions.add(hash2);
            }
            design.setHash(hashPartitions);
        }
        else if (!hashColumns2.isEmpty()) {
            throw new PrestoException(GENERIC_USER_ERROR, "Table property " + PARTITION_BY_HASH_COLUMNS_2 + " is only allowed if there is also " + PARTITION_BY_HASH_COLUMNS);
        }

        List<String> rangeColumns = (List) tableProperties.get(PARTITION_BY_RANGE_COLUMNS);
        if (!rangeColumns.isEmpty()) {
            RangePartitionDefinition range = new RangePartitionDefinition();
            range.setColumns(rangeColumns);
            design.setRange(range);
        }

        return design;
    }

    public static ColumnDesign getColumnDesign(Map<String, Object> columnProperties)
    {
        requireNonNull(columnProperties);
        if (columnProperties.isEmpty()) {
            return ColumnDesign.DEFAULT;
        }

        ColumnDesign design = new ColumnDesign();
        Boolean key = (Boolean) columnProperties.get(PRIMARY_KEY);
        if (key != null) {
            design.setPrimaryKey(key);
        }

        Boolean nullable = (Boolean) columnProperties.get(NULLABLE);
        if (nullable != null) {
            design.setNullable(nullable);
        }

        String encoding = (String) columnProperties.get(ENCODING);
        if (encoding != null) {
            design.setEncoding(encoding);
        }

        String compression = (String) columnProperties.get(COMPRESSION);
        if (compression != null) {
            design.setCompression(compression);
        }
        return design;
    }

    private static HashPartitionDefinition getHashPartitionDefinition(Map<String, Object> tableProperties, List<String> columns, String bucketPropertyName)
    {
        Integer hashBuckets = (Integer) tableProperties.get(bucketPropertyName);
        if (hashBuckets == null) {
            throw new PrestoException(GENERIC_USER_ERROR, "Missing table property " + bucketPropertyName);
        }
        HashPartitionDefinition definition = new HashPartitionDefinition();
        definition.setColumns(columns);
        definition.setBuckets(hashBuckets);
        return definition;
    }

    public static List<RangePartition> getRangePartitions(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        String json = (String) tableProperties.get(RANGE_PARTITIONS);
        if (json != null) {
            try {
                RangePartition[] partitions = mapper.readValue(json, RangePartition[].class);
                if (partitions == null) {
                    return ImmutableList.of();
                }
                return ImmutableList.copyOf(partitions);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            return ImmutableList.of();
        }
    }

    public static RangePartition parseRangePartition(String json)
    {
        if (json == null) {
            return null;
        }
        else {
            try {
                return mapper.readValue(json, RangePartition.class);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Optional<Integer> getNumReplicas(Map<String, Object> tableProperties)
    {
        requireNonNull(tableProperties);

        @SuppressWarnings("unchecked")
        Integer numReplicas = (Integer) tableProperties.get(NUM_REPLICAS);
        return Optional.ofNullable(numReplicas);
    }

    public static Map<String, Object> toMap(KuduTable table)
    {
        Map<String, Object> properties = new HashMap<>();

        LinkedHashMap<String, ColumnDesign> columns = getColumns(table);

        PartitionDesign partitionDesign = getPartitionDesign(table);

        List<RangePartition> rangePartitionList = getRangePartitionList(table, DEFAULT_DEADLINE);

        try {
            if (partitionDesign.getHash() != null) {
                List<HashPartitionDefinition> list = partitionDesign.getHash();
                if (!list.isEmpty()) {
                    properties.put(PARTITION_BY_HASH_COLUMNS, list.get(0).getColumns());
                    properties.put(PARTITION_BY_HASH_BUCKETS, list.get(0).getBuckets());
                }
                if (list.size() >= 2) {
                    properties.put(PARTITION_BY_HASH_COLUMNS_2, list.get(1).getColumns());
                    properties.put(PARTITION_BY_HASH_BUCKETS_2, list.get(1).getBuckets());
                }
            }

            if (partitionDesign.getRange() != null) {
                properties.put(PARTITION_BY_RANGE_COLUMNS, partitionDesign.getRange().getColumns());
            }

            String partitionRangesValue = mapper.writeValueAsString(rangePartitionList);
            properties.put(RANGE_PARTITIONS, partitionRangesValue);

            properties.put(NUM_REPLICAS, table.getNumReplicas());

            return properties;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<RangePartition> getRangePartitionList(KuduTable table, long deadline)
    {
        List<RangePartition> rangePartitions = new ArrayList<>();
        if (!table.getPartitionSchema().getRangeSchema().getColumns().isEmpty()) {
            try {
                Iterator var4 = table.getTabletsLocations(deadline).iterator();

                while (var4.hasNext()) {
                    LocatedTablet tablet = (LocatedTablet) var4.next();
                    Partition partition = tablet.getPartition();
                    if (Iterators.all(partition.getHashBuckets().iterator(), Predicates.equalTo(0))) {
                        RangePartition rangePartition = buildRangePartition(table, partition);
                        rangePartitions.add(rangePartition);
                    }
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return rangePartitions;
    }

    private static RangePartition buildRangePartition(KuduTable table, Partition partition)
    {
        RangeBoundValue lower = buildRangePartitionBound(table, partition.getRangeKeyStart());
        RangeBoundValue upper = buildRangePartitionBound(table, partition.getRangeKeyEnd());

        return new RangePartition(lower, upper);
    }

    private static RangeBoundValue buildRangePartitionBound(KuduTable table, byte[] rangeKey)
    {
        if (rangeKey.length == 0) {
            return null;
        }
        else {
            Schema schema = table.getSchema();
            PartitionSchema partitionSchema = table.getPartitionSchema();
            PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();
            List<Integer> rangeColumns = rangeSchema.getColumns();

            final int numColumns = rangeColumns.size();

            PartialRow bound = KeyEncoderAccessor.decodeRangePartitionKey(schema, partitionSchema, rangeKey);

            ArrayList<Object> list = new ArrayList<>();
            for (int i = 0; i < numColumns; i++) {
                Object obj = toValue(schema, bound, rangeColumns.get(i));
                list.add(obj);
            }
            return new RangeBoundValue(list);
        }
    }

    private static Object toValue(Schema schema, PartialRow bound, Integer idx)
    {
        Type type = schema.getColumnByIndex(idx).getType();
        switch (type) {
            case UNIXTIME_MICROS:
                long millis = bound.getLong(idx) / 1000;
                return ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).print(millis);
            case STRING:
                return bound.getString(idx);
            case INT64:
                return bound.getLong(idx);
            case INT32:
                return bound.getInt(idx);
            case INT16:
                return bound.getShort(idx);
            case INT8:
                return (short) bound.getByte(idx);
            case BOOL:
                return bound.getBoolean(idx);
            case BINARY:
                return bound.getBinaryCopy(idx);
            default:
                throw new IllegalStateException("Unhandled type " + type + " for range partition");
        }
    }

    private static LinkedHashMap<String, ColumnDesign> getColumns(KuduTable table)
    {
        Schema schema = table.getSchema();
        LinkedHashMap<String, ColumnDesign> columns = new LinkedHashMap<>();
        for (ColumnSchema columnSchema : schema.getColumns()) {
            ColumnDesign design = new ColumnDesign();
            design.setNullable(columnSchema.isNullable());
            design.setPrimaryKey(columnSchema.isKey());
            design.setCompression(lookupCompressionString(columnSchema.getCompressionAlgorithm()));
            design.setEncoding(lookupEncodingString(columnSchema.getEncoding()));
            columns.put(columnSchema.getName(), design);
        }
        return columns;
    }

    public static PartitionDesign getPartitionDesign(KuduTable table)
    {
        Schema schema = table.getSchema();
        PartitionDesign partitionDesign = new PartitionDesign();
        PartitionSchema partitionSchema = table.getPartitionSchema();

        List<HashPartitionDefinition> hashPartitions = partitionSchema.getHashBucketSchemas().stream()
                .map(hashBucketSchema -> {
                    HashPartitionDefinition hash = new HashPartitionDefinition();
                    List<String> cols = hashBucketSchema.getColumnIds().stream()
                            .map(idx -> schema.getColumnByIndex(idx).getName()).collect(toImmutableList());
                    hash.setColumns(cols);
                    hash.setBuckets(hashBucketSchema.getNumBuckets());
                    return hash;
                }).collect(toImmutableList());
        partitionDesign.setHash(hashPartitions);

        List<Integer> rangeColumns = partitionSchema.getRangeSchema().getColumns();
        if (!rangeColumns.isEmpty()) {
            RangePartitionDefinition definition = new RangePartitionDefinition();
            definition.setColumns(rangeColumns.stream()
                    .map(i -> schema.getColumns().get(i).getName())
                    .collect(ImmutableList.toImmutableList()));
            partitionDesign.setRange(definition);
        }

        return partitionDesign;
    }

    public static PartialRow toRangeBoundToPartialRow(Schema schema, RangePartitionDefinition definition,
            RangeBoundValue boundValue)
    {
        PartialRow partialRow = new PartialRow(schema);
        if (boundValue != null) {
            List<Integer> rangeColumns = definition.getColumns().stream()
                    .map(schema::getColumnIndex).collect(toImmutableList());

            if (rangeColumns.size() != boundValue.getValues().size()) {
                throw new IllegalStateException("Expected " + rangeColumns.size()
                        + " range columns, but got " + boundValue.getValues().size());
            }
            for (int i = 0; i < rangeColumns.size(); i++) {
                Object obj = boundValue.getValues().get(i);
                int idx = rangeColumns.get(i);
                ColumnSchema columnSchema = schema.getColumnByIndex(idx);
                setColumnValue(partialRow, idx, obj, columnSchema.getType(), columnSchema.getName());
            }
        }
        return partialRow;
    }

    private static void setColumnValue(PartialRow partialRow, int idx, Object obj, Type type, String name)
    {
        Number n;

        switch (type) {
            case STRING:
                if (obj instanceof String) {
                    partialRow.addString(idx, (String) obj);
                }
                else {
                    handleInvalidValue(name, type, obj);
                }
                break;
            case INT64:
                n = toNumber(obj, type, name);
                partialRow.addLong(idx, n.longValue());
                break;
            case INT32:
                n = toNumber(obj, type, name);
                partialRow.addInt(idx, n.intValue());
                break;
            case INT16:
                n = toNumber(obj, type, name);
                partialRow.addShort(idx, n.shortValue());
                break;
            case INT8:
                n = toNumber(obj, type, name);
                partialRow.addByte(idx, n.byteValue());
                break;
            case DOUBLE:
                n = toNumber(obj, type, name);
                partialRow.addDouble(idx, n.doubleValue());
                break;
            case FLOAT:
                n = toNumber(obj, type, name);
                partialRow.addFloat(idx, n.floatValue());
                break;
            case UNIXTIME_MICROS:
                long l = toUnixTimeMicros(obj, type, name);
                partialRow.addLong(idx, l);
                break;
            case BOOL:
                boolean b = toBoolean(obj, type, name);
                partialRow.addBoolean(idx, b);
                break;
            case BINARY:
                byte[] bytes = toByteArray(obj, type, name);
                partialRow.addBinary(idx, bytes);
                break;
            default:
                handleInvalidValue(name, type, obj);
                break;
        }
    }

    private static byte[] toByteArray(Object obj, Type type, String name)
    {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        else if (obj instanceof String) {
            return Base64.getDecoder().decode((String) obj);
        }
        else {
            handleInvalidValue(name, type, obj);
            return null;
        }
    }

    private static boolean toBoolean(Object obj, Type type, String name)
    {
        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }
        else if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        }
        else {
            handleInvalidValue(name, type, obj);
            return false;
        }
    }

    private static long toUnixTimeMicros(Object obj, Type type, String name)
    {
        if (Number.class.isAssignableFrom(obj.getClass())) {
            return ((Number) obj).longValue();
        }
        else if (obj instanceof String) {
            String s = (String) obj;
            s = s.trim().replace(' ', 'T');
            long millis = ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC).parseMillis(s);
            return millis * 1000;
        }
        else {
            handleInvalidValue(name, type, obj);
            return 0;
        }
    }

    private static Number toNumber(Object obj, Type type, String name)
    {
        if (Number.class.isAssignableFrom(obj.getClass())) {
            return (Number) obj;
        }
        else if (obj instanceof String) {
            String s = (String) obj;
            BigDecimal d = new BigDecimal((String) obj);
            return d;
        }
        else {
            handleInvalidValue(name, type, obj);
            return 0;
        }
    }

    private static void handleInvalidValue(String name, Type type, Object obj)
    {
        throw new IllegalStateException("Invalid value " + obj + " for column " + name + " of type " + type);
    }

    public static ColumnSchema.CompressionAlgorithm lookupCompression(String compression)
    {
        switch (compression.toLowerCase(Locale.ENGLISH)) {
            case "default":
            case "default_compression":
                return ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION;
            case "no":
            case "no_compression":
                return ColumnSchema.CompressionAlgorithm.NO_COMPRESSION;
            case "lz4":
                return ColumnSchema.CompressionAlgorithm.LZ4;
            case "snappy":
                return ColumnSchema.CompressionAlgorithm.SNAPPY;
            case "zlib":
                return ColumnSchema.CompressionAlgorithm.ZLIB;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static String lookupCompressionString(ColumnSchema.CompressionAlgorithm algorithm)
    {
        switch (algorithm) {
            case DEFAULT_COMPRESSION:
                return "default";
            case NO_COMPRESSION:
                return "no";
            case LZ4:
                return "lz4";
            case SNAPPY:
                return "snappy";
            case ZLIB:
                return "zlib";
            default:
                return "unknown";
        }
    }

    public static ColumnSchema.Encoding lookupEncoding(String encoding)
    {
        switch (encoding.toLowerCase(Locale.ENGLISH)) {
            case "auto":
            case "auto_encoding":
                return ColumnSchema.Encoding.AUTO_ENCODING;
            case "bitshuffle":
            case "bit_shuffle":
                return ColumnSchema.Encoding.BIT_SHUFFLE;
            case "dictionary":
            case "dict_encoding":
                return ColumnSchema.Encoding.DICT_ENCODING;
            case "plain":
            case "plain_encoding":
                return ColumnSchema.Encoding.PLAIN_ENCODING;
            case "prefix":
            case "prefix_encoding":
                return ColumnSchema.Encoding.PREFIX_ENCODING;
            case "runlength":
            case "run_length":
            case "run length":
            case "rle":
                return ColumnSchema.Encoding.RLE;
            case "group_varint":
                return ColumnSchema.Encoding.GROUP_VARINT;
            default:
                throw new IllegalArgumentException();
        }
    }

    public static String lookupEncodingString(ColumnSchema.Encoding encoding)
    {
        switch (encoding) {
            case AUTO_ENCODING:
                return "auto";
            case BIT_SHUFFLE:
                return "bitshuffle";
            case DICT_ENCODING:
                return "dictionary";
            case PLAIN_ENCODING:
                return "plain";
            case PREFIX_ENCODING:
                return "prefix";
            case RLE:
                return "runlength";
            case GROUP_VARINT:
                return "group_varint";
            default:
                return "unknown";
        }
    }
}
