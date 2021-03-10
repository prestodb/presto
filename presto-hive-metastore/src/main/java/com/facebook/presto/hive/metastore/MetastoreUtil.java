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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.PartitionOfflineException;
import com.facebook.presto.hive.TableOfflineException;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.Chars.padSpaces;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.typeToThriftType;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_COUNT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.BUCKET_FIELD_NAME;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.joda.time.DateTimeZone.UTC;

public class MetastoreUtil
{
    public static final String PRESTO_OFFLINE = "presto_offline";
    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";
    public static final String PRESTO_VIEW_FLAG = "presto_view";
    public static final String PRESTO_QUERY_ID_NAME = "presto_query_id";
    public static final String HIVE_DEFAULT_DYNAMIC_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    @SuppressWarnings("OctalInteger")
    public static final FsPermission ALL_PERMISSIONS = new FsPermission((short) 0777);

    private static final String PARTITION_VALUE_WILDCARD = "";
    private static final String NUM_FILES = "numFiles";
    private static final String NUM_ROWS = "numRows";
    private static final String RAW_DATA_SIZE = "rawDataSize";
    private static final String TOTAL_SIZE = "totalSize";
    private static final Set<String> STATS_PROPERTIES = ImmutableSet.of(NUM_FILES, NUM_ROWS, RAW_DATA_SIZE, TOTAL_SIZE);

    private MetastoreUtil()
    {
    }

    public static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static boolean isRowType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }

    public static Properties getHiveSchema(Table table)
    {
        // Mimics function in Hive: MetaStoreUtils.getTableMetadata(Table)
        return getHiveSchema(
                table.getStorage(),
                table.getDataColumns(),
                table.getDataColumns(),
                table.getParameters(),
                table.getDatabaseName(),
                table.getTableName(),
                table.getPartitionColumns());
    }

    public static Properties getHiveSchema(Partition partition, Table table)
    {
        // Mimics function in Hive: MetaStoreUtils.getSchema(Partition, Table)
        return getHiveSchema(
                partition.getStorage(),
                partition.getColumns(),
                table.getDataColumns(),
                table.getParameters(),
                table.getDatabaseName(),
                table.getTableName(),
                table.getPartitionColumns());
    }

    public static Properties getHiveSchema(
            Storage storage,
            List<Column> partitionDataColumns,
            List<Column> tableDataColumns,
            Map<String, String> tableParameters,
            String databaseName,
            String tableName,
            List<Column> partitionKeys)
    {
        // Mimics function in Hive:
        // MetaStoreUtils.getSchema(StorageDescriptor, StorageDescriptor, Map<String, String>, String, String, List<FieldSchema>)

        Properties schema = new Properties();

        schema.setProperty(FILE_INPUT_FORMAT, storage.getStorageFormat().getInputFormat());
        schema.setProperty(FILE_OUTPUT_FORMAT, storage.getStorageFormat().getOutputFormat());

        schema.setProperty(META_TABLE_NAME, databaseName + "." + tableName);
        schema.setProperty(META_TABLE_LOCATION, storage.getLocation());

        if (storage.getBucketProperty().isPresent()) {
            List<String> bucketedBy = storage.getBucketProperty().get().getBucketedBy();
            if (!bucketedBy.isEmpty()) {
                schema.setProperty(BUCKET_FIELD_NAME, bucketedBy.get(0));
            }
            schema.setProperty(BUCKET_COUNT, Integer.toString(storage.getBucketProperty().get().getBucketCount()));
        }
        else {
            schema.setProperty(BUCKET_COUNT, "0");
        }

        for (Entry<String, String> param : storage.getSerdeParameters().entrySet()) {
            schema.setProperty(param.getKey(), (param.getValue() != null) ? param.getValue() : "");
        }
        schema.setProperty(SERIALIZATION_LIB, storage.getStorageFormat().getSerDe());

        StringBuilder columnNameBuilder = new StringBuilder();
        StringBuilder columnTypeBuilder = new StringBuilder();
        StringBuilder columnCommentBuilder = new StringBuilder();
        boolean first = true;
        for (Column column : tableDataColumns) {
            if (!first) {
                columnNameBuilder.append(",");
                columnTypeBuilder.append(":");
                columnCommentBuilder.append('\0');
            }
            columnNameBuilder.append(column.getName());
            columnTypeBuilder.append(column.getType());
            columnCommentBuilder.append(column.getComment().orElse(""));
            first = false;
        }
        String columnNames = columnNameBuilder.toString();
        String columnTypes = columnTypeBuilder.toString();
        schema.setProperty(META_TABLE_COLUMNS, columnNames);
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes);
        schema.setProperty("columns.comments", columnCommentBuilder.toString());

        schema.setProperty(SERIALIZATION_DDL, toThriftDdl(tableName, partitionDataColumns));

        String partString = "";
        String partStringSep = "";
        String partTypesString = "";
        String partTypesStringSep = "";
        for (Column partKey : partitionKeys) {
            partString += partStringSep;
            partString += partKey.getName();
            partTypesString += partTypesStringSep;
            partTypesString += partKey.getType().getHiveTypeName().toString();
            if (partStringSep.length() == 0) {
                partStringSep = "/";
                partTypesStringSep = ":";
            }
        }
        if (partString.length() > 0) {
            schema.setProperty(META_TABLE_PARTITION_COLUMNS, partString);
            schema.setProperty(META_TABLE_PARTITION_COLUMN_TYPES, partTypesString);
        }

        if (tableParameters != null) {
            for (Entry<String, String> entry : tableParameters.entrySet()) {
                // add non-null parameters to the schema
                if (entry.getValue() != null) {
                    schema.setProperty(entry.getKey(), entry.getValue());
                }
            }
        }

        return schema;
    }

    public static Properties getHiveSchema(Map<String, String> serdeParameters, Map<String, String> tableParameters)
    {
        Properties schema = new Properties();
        for (Entry<String, String> param : serdeParameters.entrySet()) {
            schema.setProperty(param.getKey(), (param.getValue() != null) ? param.getValue() : "");
        }
        for (Entry<String, String> entry : tableParameters.entrySet()) {
            // add non-null parameters to the schema
            if (entry.getValue() != null) {
                schema.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return schema;
    }

    /**
     * Recreates partition schema based on the table schema and the column
     * coercions map.
     * <p>
     * partitionColumnCount is needed to handle cases when the partition
     * has less columns than the table.
     * <p>
     * If the partition has more columns than the table does, the partitionSchemaDifference
     * map is expected to contain information for the missing columns.
     */
    public static List<Column> reconstructPartitionSchema(List<Column> tableSchema, int partitionColumnCount, Map<Integer, Column> partitionSchemaDifference)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (int i = 0; i < partitionColumnCount; i++) {
            Column column = partitionSchemaDifference.get(i);
            if (column == null) {
                checkArgument(
                        i < tableSchema.size(),
                        "column descriptor for column with hiveColumnIndex %s not found: tableSchema: %s, partitionSchemaDifference: %s",
                        i,
                        tableSchema,
                        partitionSchemaDifference);
                column = tableSchema.get(i);
            }
            columns.add(column);
        }
        return columns.build();
    }

    public static ProtectMode getProtectMode(Partition partition)
    {
        return getProtectMode(partition.getParameters());
    }

    public static ProtectMode getProtectMode(Table table)
    {
        return getProtectMode(table.getParameters());
    }

    public static String makePartName(List<Column> partitionColumns, List<String> values)
    {
        checkArgument(partitionColumns.size() == values.size(), "Partition value count does not match the partition column count");
        checkArgument(values.stream().allMatch(Objects::nonNull), "partitionValue must not have null elements");

        List<String> partitionColumnNames = partitionColumns.stream().map(Column::getName).collect(toList());
        return FileUtils.makePartName(partitionColumnNames, values);
    }

    public static String getPartitionLocation(Table table, Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return table.getStorage().getLocation();
        }
        return partition.get().getStorage().getLocation();
    }

    private static String toThriftDdl(String structName, List<Column> columns)
    {
        // Mimics function in Hive:
        // MetaStoreUtils.getDDLFromFieldSchema(String, List<FieldSchema>)
        StringBuilder ddl = new StringBuilder();
        ddl.append("struct ");
        ddl.append(structName);
        ddl.append(" { ");
        boolean first = true;
        for (Column column : columns) {
            if (first) {
                first = false;
            }
            else {
                ddl.append(", ");
            }
            ddl.append(typeToThriftType(column.getType().getHiveTypeName().toString()));
            ddl.append(' ');
            ddl.append(column.getName());
        }
        ddl.append("}");
        return ddl.toString();
    }

    private static ProtectMode getProtectMode(Map<String, String> parameters)
    {
        if (!parameters.containsKey(ProtectMode.PARAMETER_NAME)) {
            return new ProtectMode();
        }
        else {
            return getProtectModeFromString(parameters.get(ProtectMode.PARAMETER_NAME));
        }
    }

    public static void verifyOnline(SchemaTableName tableName, Optional<String> partitionName, ProtectMode protectMode, Map<String, String> parameters)
    {
        if (protectMode.offline) {
            if (partitionName.isPresent()) {
                throw new PartitionOfflineException(tableName, partitionName.get(), false, null);
            }
            throw new TableOfflineException(tableName, false, null);
        }

        String prestoOffline = parameters.get(PRESTO_OFFLINE);
        if (!isNullOrEmpty(prestoOffline)) {
            if (partitionName.isPresent()) {
                throw new PartitionOfflineException(tableName, partitionName.get(), true, prestoOffline);
            }
            throw new TableOfflineException(tableName, true, prestoOffline);
        }
    }

    public static void verifyCanDropColumn(ExtendedHiveMetastore metastore, String databaseName, String tableName, String columnName)
    {
        Table table = metastore.getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        if (table.getPartitionColumns().stream().anyMatch(column -> column.getName().equals(columnName))) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop partition columns");
        }
        if (table.getDataColumns().size() <= 1) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot drop the only non-partition column in a table");
        }
    }

    public static FileSystem getFileSystem(HdfsEnvironment hdfsEnvironment, HdfsContext context, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(context, path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Error getting file system. Path: %s", path), e);
        }
    }

    public static void renameFile(FileSystem fileSystem, Path source, Path target)
    {
        try {
            if (fileSystem.exists(target) || !fileSystem.rename(source, target)) {
                throw new PrestoException(HIVE_FILESYSTEM_ERROR, getRenameErrorMessage(source, target));
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, getRenameErrorMessage(source, target), e);
        }
    }

    public static List<String> toPartitionValues(String partitionName)
    {
        // mimics Warehouse.makeValsFromName
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        int start = 0;
        while (true) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            if (start > partitionName.length()) {
                break;
            }
            resultBuilder.add(unescapePathName(partitionName.substring(start, end)));
            start = end + 1;
        }
        return resultBuilder.build();
    }

    public static List<String> extractPartitionValues(String partitionName)
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();

        boolean inKey = true;
        int valueStart = -1;
        for (int i = 0; i < partitionName.length(); i++) {
            char current = partitionName.charAt(i);
            if (inKey) {
                checkArgument(current != '/', "Invalid partition spec: %s", partitionName);
                if (current == '=') {
                    inKey = false;
                    valueStart = i + 1;
                }
            }
            else if (current == '/') {
                checkArgument(valueStart != -1, "Invalid partition spec: %s", partitionName);
                values.add(unescapePathName(partitionName.substring(valueStart, i)));
                inKey = true;
                valueStart = -1;
            }
        }
        checkArgument(!inKey, "Invalid partition spec: %s", partitionName);
        values.add(unescapePathName(partitionName.substring(valueStart, partitionName.length())));

        return values.build();
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            Object value = getField(partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            if (value == null) {
                partitionValues.add(HIVE_DEFAULT_DYNAMIC_PARTITION);
            }
            else {
                String valueString = value.toString();
                if (!CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(valueString)) {
                    throw new PrestoException(HIVE_INVALID_PARTITION_VALUE,
                            "Hive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: " +
                                    base16().withSeparator(" ", 2).encode(valueString.getBytes(UTF_8)));
                }
                partitionValues.add(valueString);
            }
        }
        return partitionValues.build();
    }

    public static Object getField(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BigintType.BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return (int) type.getLong(block, position);
        }
        if (SmallintType.SMALLINT.equals(type)) {
            return (short) type.getLong(block, position);
        }
        if (TinyintType.TINYINT.equals(type)) {
            return (byte) type.getLong(block, position);
        }
        if (RealType.REAL.equals(type)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return new Text(type.getSlice(block, position).getBytes());
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return new Text(padEnd(type.getSlice(block, position).toStringUtf8(), charType.getLength(), ' '));
        }
        if (VarbinaryType.VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DateType.DATE.equals(type)) {
            long days = type.getLong(block, position);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), TimeUnit.DAYS.toMillis(days)));
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Timestamp(millisUtc);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getHiveDecimal(decimalType, block, position);
        }
        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);

            Block arrayBlock = block.getBlock(position);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getField(elementType, arrayBlock, i);
                list.add(element);
            }

            return Collections.unmodifiableList(list);
        }
        if (isMapType(type)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            Block mapBlock = block.getBlock(position);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Object key = getField(keyType, mapBlock, i);
                Object value = getField(valueType, mapBlock, i + 1);
                map.put(key, value);
            }

            return Collections.unmodifiableMap(map);
        }
        if (isRowType(type)) {
            Block rowBlock = block.getBlock(position);

            List<Type> fieldTypes = type.getTypeParameters();
            checkCondition(fieldTypes.size() == rowBlock.getPositionCount(), StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");

            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                Object element = getField(fieldTypes.get(i), rowBlock, i);
                row.add(element);
            }

            return Collections.unmodifiableList(row);
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public static HiveDecimal getHiveDecimal(DecimalType decimalType, Block block, int position)
    {
        BigInteger unscaledValue;
        if (decimalType.isShort()) {
            unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
        }
        else {
            unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
        }
        return HiveDecimal.create(unscaledValue, decimalType.getScale());
    }

    public static void createDirectory(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(context, path).mkdirs(path, ALL_PERMISSIONS)) {
                throw new IOException("mkdirs returned false");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + path, e);
        }

        // explicitly set permission since the default umask overrides it on creation
        try {
            hdfsEnvironment.getFileSystem(context, path).setPermission(path, ALL_PERMISSIONS);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to set permission on directory: " + path, e);
        }
    }

    public static boolean pathExists(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(context, path).exists(path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    private static String getRenameErrorMessage(Path source, Path target)
    {
        return format("Error moving data files from %s to final location %s", source, target);
    }

    public static List<String> convertPredicateToParts(Map<Column, Domain> partitionPredicates)
    {
        List<String> filter = new ArrayList<>();

        for (Entry<Column, Domain> partitionPredicate : partitionPredicates.entrySet()) {
            Domain domain = partitionPredicate.getValue();
            if (!domain.isAll()) {
                if (domain != null && domain.isNullableSingleValue()) {
                    Object value = domain.getNullableSingleValue();
                    Type type = domain.getType();
                    filter.add(convertRawValueToString(value, type));
                }
                else {
                    filter.add(PARTITION_VALUE_WILDCARD);
                }
            }
            else {
                filter.add(PARTITION_VALUE_WILDCARD);
            }
        }

        return filter;
    }

    public static String convertRawValueToString(Object value, Type type)
    {
        String val;
        if (value == null) {
            val = HIVE_DEFAULT_DYNAMIC_PARTITION;
        }
        else if (type instanceof CharType) {
            Slice slice = (Slice) value;
            val = padSpaces(slice, type).toStringUtf8();
        }
        else if (type instanceof VarcharType) {
            Slice slice = (Slice) value;
            val = slice.toStringUtf8();
        }
        else if (type instanceof DecimalType && !((DecimalType) type).isShort()) {
            Slice slice = (Slice) value;
            val = Decimals.toString(slice, ((DecimalType) type).getScale());
        }
        else if (type instanceof DecimalType && ((DecimalType) type).isShort()) {
            val = Decimals.toString((long) value, ((DecimalType) type).getScale());
        }
        else if (type instanceof DateType) {
            DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.date().withZoneUTC();
            val = dateTimeFormatter.print(TimeUnit.DAYS.toMillis((long) value));
        }
        else if (type instanceof TimestampType) {
            // we don't have time zone info, so just add a wildcard
            val = PARTITION_VALUE_WILDCARD;
        }
        else if (type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType
                || type instanceof DoubleType
                || type instanceof RealType
                || type instanceof BooleanType) {
            val = value.toString();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported partition key type: %s", type.getDisplayName()));
        }
        return val;
    }

    /**
     * Hive calculates NDV considering null as a distinct value
     */
    public static OptionalLong fromMetastoreDistinctValuesCount(OptionalLong distinctValuesCount, OptionalLong nullsCount, OptionalLong rowCount)
    {
        if (distinctValuesCount.isPresent() && nullsCount.isPresent() && rowCount.isPresent()) {
            return OptionalLong.of(fromMetastoreDistinctValuesCount(distinctValuesCount.getAsLong(), nullsCount.getAsLong(), rowCount.getAsLong()));
        }
        return OptionalLong.empty();
    }

    public static long fromMetastoreDistinctValuesCount(long distinctValuesCount, long nullsCount, long rowCount)
    {
        long nonNullsCount = rowCount - nullsCount;
        if (nullsCount > 0 && distinctValuesCount > 0) {
            distinctValuesCount--;
        }

        // normalize distinctValuesCount in case there is a non null element
        if (nonNullsCount > 0 && distinctValuesCount == 0) {
            distinctValuesCount = 1;
        }

        // the metastore may store an estimate, so the value stored may be higher than the total number of rows
        if (distinctValuesCount > nonNullsCount) {
            return nonNullsCount;
        }
        return distinctValuesCount;
    }

    public static Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        if (isNumericType(type) || type.equals(DATE) || type.equals(TIMESTAMP)) {
            // TODO #7122 support non-legacy TIMESTAMP
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (isVarcharType(type) || isCharType(type)) {
            // TODO Collect MIN,MAX once it is used by the optimizer
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES);
        }
        // Throwing here to make sure this method is updated when a new type is added in Hive connector
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    public static HiveBasicStatistics getHiveBasicStatistics(Map<String, String> parameters)
    {
        OptionalLong numFiles = parse(parameters.get(NUM_FILES));
        OptionalLong numRows = parse(parameters.get(NUM_ROWS));
        OptionalLong inMemoryDataSizeInBytes = parse(parameters.get(RAW_DATA_SIZE));
        OptionalLong onDiskDataSizeInBytes = parse(parameters.get(TOTAL_SIZE));
        return new HiveBasicStatistics(numFiles, numRows, inMemoryDataSizeInBytes, onDiskDataSizeInBytes);
    }

    private static OptionalLong parse(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return OptionalLong.empty();
        }
        Long longValue = Longs.tryParse(parameterValue);
        if (longValue == null || longValue < 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(longValue);
    }

    public static Map<String, String> updateStatisticsParameters(Map<String, String> parameters, HiveBasicStatistics statistics)
    {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        parameters.forEach((key, value) -> {
            if (!STATS_PROPERTIES.contains(key)) {
                result.put(key, value);
            }
        });

        statistics.getFileCount().ifPresent(count -> result.put(NUM_FILES, Long.toString(count)));
        statistics.getRowCount().ifPresent(count -> result.put(NUM_ROWS, Long.toString(count)));
        statistics.getInMemoryDataSizeInBytes().ifPresent(size -> result.put(RAW_DATA_SIZE, Long.toString(size)));
        statistics.getOnDiskDataSizeInBytes().ifPresent(size -> result.put(TOTAL_SIZE, Long.toString(size)));

        return result.build();
    }
}
