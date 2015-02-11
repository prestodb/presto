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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.HiveType.HIVE_BOOLEAN;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.HiveType.getHiveType;
import static com.facebook.presto.hive.HiveType.getSupportedHiveType;
import static com.facebook.presto.hive.HiveType.getType;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.getTableMetadata;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public final class HiveUtil
{
    public static final String PRESTO_VIEW_FLAG = "presto_view";

    private static final String VIEW_PREFIX = "/* Presto View: ";
    private static final String VIEW_SUFFIX = " */";

    private static final DateTimeFormatter HIVE_DATE_PARSER = ISODateTimeFormat.date().withZoneUTC();
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER;

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSSSSSSS").getParser(),
        };
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS").getPrinter();
        HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder().append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser).toFormatter().withZoneUTC();
    }

    private HiveUtil()
    {
    }

    public static RecordReader<?, ?> createRecordReader(String clientId, Configuration configuration, Path path, long start, long length, Properties schema, List<HiveColumnHandle> columns, TypeManager typeManager)
    {
        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, not(HiveColumnHandle::isPartitionKey)));
        List<Integer> readHiveColumnIndexes = ImmutableList.copyOf(transform(readColumns, HiveColumnHandle::getHiveColumnIndex));

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        setReadColumns(configuration, readHiveColumnIndexes);

        final InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, true);
        final JobConf jobConf = new JobConf(configuration);
        final FileSplit fileSplit = new FileSplit(path, start, length, (String[]) null);

        // propagate serialization configuration to getRecordReader
        for (String name : schema.stringPropertyNames()) {
            if (name.startsWith("serialization.")) {
                jobConf.set(name, schema.getProperty(name));
            }
        }

        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("createRecordReader", () -> inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL));
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, format("Error opening Hive split %s (offset=%s, length=%s) using %s: %s",
                    path,
                    start,
                    length,
                    getInputFormatName(schema),
                    e.getMessage()),
                    e);
        }
    }

    public static void setReadColumns(Configuration configuration, List<Integer> readHiveColumnIndexes)
    {
        configuration.set(READ_COLUMN_IDS_CONF_STR, Joiner.on(',').join(readHiveColumnIndexes));
        configuration.setBoolean(READ_ALL_COLUMNS, false);
    }

    static InputFormat<?, ?> getInputFormat(Configuration configuration, Properties schema, boolean symlinkTarget)
    {
        String inputFormatName = getInputFormatName(schema);
        try {
            JobConf jobConf = new JobConf(configuration);

            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (ClassNotFoundException | RuntimeException e) {
            throw new RuntimeException("Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
            throws ClassNotFoundException
    {
        // CDH uses different names for Parquet
        if ("parquet.hive.DeprecatedParquetInputFormat".equals(inputFormatName) ||
            "parquet.hive.MapredParquetInputFormat".equals(inputFormatName)) {
            return MapredParquetInputFormat.class;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        // TODO: remove redundant cast to Object after IDEA-118533 is fixed
        return (Class<? extends InputFormat<?, ?>>) (Object) clazz.asSubclass(InputFormat.class);
    }

    static String getInputFormatName(Properties schema)
    {
        String name = schema.getProperty(FILE_INPUT_FORMAT);
        checkCondition(name != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive input format property: %s", FILE_INPUT_FORMAT);
        return name;
    }

    public static long parseHiveDate(String value)
    {
        long millis = HIVE_DATE_PARSER.parseMillis(value);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    public static long parseHiveTimestamp(String value, DateTimeZone timeZone)
    {
        return HIVE_TIMESTAMP_PARSER.withZone(timeZone).parseMillis(value);
    }

    static boolean isSplittable(InputFormat<?, ?> inputFormat, FileSystem fileSystem, Path path)
    {
        // use reflection to get isSplittable method on InputFormat
        Method method = null;
        for (Class<?> clazz = inputFormat.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            try {
                method = clazz.getDeclaredMethod("isSplitable", FileSystem.class, Path.class);
                break;
            }
            catch (NoSuchMethodException ignored) {
            }
        }

        if (method == null) {
            return false;
        }
        try {
            method.setAccessible(true);
            return (boolean) method.invoke(inputFormat, fileSystem, path);
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
    }

    public static StructObjectInspector getTableObjectInspector(Properties schema)
    {
        return getTableObjectInspector(getDeserializer(schema));
    }

    public static StructObjectInspector getTableObjectInspector(Deserializer deserializer)
    {
        try {
            ObjectInspector inspector = deserializer.getObjectInspector();
            checkArgument(inspector.getCategory() == Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
            return (StructObjectInspector) inspector;
        }
        catch (SerDeException e) {
            throw Throwables.propagate(e);
        }
    }

    public static List<? extends StructField> getTableStructFields(Table table)
    {
        return getTableObjectInspector(getTableMetadata(table)).getAllStructFieldRefs();
    }

    @SuppressWarnings("deprecation")
    public static Deserializer getDeserializer(Properties schema)
    {
        String name = schema.getProperty(SERIALIZATION_LIB);
        checkCondition(name != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive deserializer property: %s", SERIALIZATION_LIB);

        Deserializer deserializer = createDeserializer(getDeserializerClass(name));
        initializeDeserializer(deserializer, schema);
        return deserializer;
    }

    @SuppressWarnings("deprecation")
    private static Class<? extends Deserializer> getDeserializerClass(String name)
    {
        // CDH uses different names for Parquet
        if ("parquet.hive.serde.ParquetHiveSerDe".equals(name)) {
            return ParquetHiveSerDe.class;
        }

        try {
            return Class.forName(name, true, JavaUtils.getClassLoader()).asSubclass(Deserializer.class);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("deserializer does not exist: " + name);
        }
        catch (ClassCastException e) {
            throw new RuntimeException("invalid deserializer class: " + name);
        }
    }

    @SuppressWarnings("deprecation")
    private static Deserializer createDeserializer(Class<? extends Deserializer> clazz)
    {
        try {
            return clazz.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("error creating deserializer: " + clazz.getName(), e);
        }
    }

    @SuppressWarnings("deprecation")
    private static void initializeDeserializer(Deserializer deserializer, Properties schema)
    {
        try {
            deserializer.initialize(null, schema);
        }
        catch (SerDeException e) {
            throw new RuntimeException("error initializing deserializer: " + deserializer.getClass().getName());
        }
    }

    public static boolean isHiveNull(byte[] bytes)
    {
        return bytes.length == 2 && bytes[0] == '\\' && bytes[1] == 'N';
    }

    public static SerializableNativeValue parsePartitionValue(String partitionName, String value, HiveType hiveType, DateTimeZone timeZone)
    {
        try {
            boolean isNull = HIVE_DEFAULT_DYNAMIC_PARTITION.equals(value);

            if (HIVE_BOOLEAN.equals(hiveType)) {
                if (isNull) {
                    return new SerializableNativeValue(Boolean.class, null);
                }
                if (value.isEmpty()) {
                    return new SerializableNativeValue(Boolean.class, false);
                }
                return new SerializableNativeValue(Boolean.class, parseBoolean(value));
            }

            if (HIVE_BYTE.equals(hiveType) || HIVE_SHORT.equals(hiveType) || HIVE_INT.equals(hiveType) || HIVE_LONG.equals(hiveType)) {
                if (isNull) {
                    return new SerializableNativeValue(Long.class, null);
                }
                if (value.isEmpty()) {
                    return new SerializableNativeValue(Long.class, 0L);
                }
                return new SerializableNativeValue(Long.class, parseLong(value));
            }

            if (HIVE_DATE.equals(hiveType)) {
                if (isNull) {
                    return new SerializableNativeValue(Long.class, null);
                }
                long dateInMillis = parseHiveDate(value);
                return new SerializableNativeValue(Long.class, dateInMillis);
            }

            if (HIVE_TIMESTAMP.equals(hiveType)) {
                if (isNull) {
                    return new SerializableNativeValue(Long.class, null);
                }
                return new SerializableNativeValue(Long.class, parseHiveTimestamp(value, timeZone));
            }

            if (HIVE_FLOAT.equals(hiveType) || HIVE_DOUBLE.equals(hiveType)) {
                if (isNull) {
                    return new SerializableNativeValue(Double.class, null);
                }
                if (value.isEmpty()) {
                    return new SerializableNativeValue(Double.class, 0.0);
                }
                return new SerializableNativeValue(Double.class, parseDouble(value));
            }

            if (HIVE_STRING.equals(hiveType)) {
                if (isNull) {
                    return new SerializableNativeValue(Slice.class, null);
                }
                return new SerializableNativeValue(Slice.class, utf8Slice(value));
            }
        }
        catch (RuntimeException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format(
                    "Invalid partition value '%s' for Hive type [%s] partition key: %s", value, hiveType, partitionName));
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type [%s] for partition: %s", hiveType, partitionName));
    }

    public static boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    public static String encodeViewData(String data)
    {
        return VIEW_PREFIX + Base64.getEncoder().encodeToString(data.getBytes(UTF_8)) + VIEW_SUFFIX;
    }

    public static String decodeViewData(String data)
    {
        checkCondition(data.startsWith(VIEW_PREFIX), HIVE_INVALID_VIEW_DATA, "View data missing prefix: %s", data);
        checkCondition(data.endsWith(VIEW_SUFFIX), HIVE_INVALID_VIEW_DATA, "View data missing suffix: %s", data);
        data = data.substring(VIEW_PREFIX.length());
        data = data.substring(0, data.length() - VIEW_SUFFIX.length());
        return new String(Base64.getDecoder().decode(data), UTF_8);
    }

    public static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static boolean isStructuralType(HiveType hiveType)
    {
        return hiveType.getCategory() == Category.LIST || hiveType.getCategory() == Category.MAP || hiveType.getCategory() == Category.STRUCT;
    }

    public static boolean booleanPartitionKey(String value, String name)
    {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for BOOLEAN partition key: %s", value, name));
    }

    public static long bigintPartitionKey(String value, String name)
    {
        try {
            return parseLong(value);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for BIGINT partition key: %s", value, name));
        }
    }

    public static double doublePartitionKey(String value, String name)
    {
        try {
            return parseDouble(value);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for DOUBLE partition key: %s", value, name));
        }
    }

    public static long datePartitionKey(String value, String name)
    {
        try {
            return parseHiveDate(value);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for DATE partition key: %s", value, name));
        }
    }

    public static long timestampPartitionKey(String value, DateTimeZone zone, String name)
    {
        try {
            return parseHiveTimestamp(value, zone);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for TIMESTAMP partition key: %s", value, name));
        }
    }

    public static SchemaTableName schemaTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, HiveTableHandle.class, "tableHandle").getSchemaTableName();
    }

    public static List<HiveColumnHandle> hiveColumnHandles(TypeManager typeManager, String connectorId, Table table, boolean includeSampleWeight)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        // add the data fields first
        int hiveColumnIndex = 0;
        for (StructField field : getTableStructFields(table)) {
            // ignore unsupported types rather than failing
            HiveType hiveType = getHiveType(field.getFieldObjectInspector());
            if (hiveType != null && (includeSampleWeight || !field.getFieldName().equals(SAMPLE_WEIGHT_COLUMN_NAME))) {
                Type type = getType(field.getFieldObjectInspector(), typeManager);
                checkCondition(type != null, NOT_SUPPORTED, "Unsupported Hive type: %s", field.getFieldObjectInspector().getTypeName());
                columns.add(new HiveColumnHandle(connectorId, field.getFieldName(), hiveColumnIndex, hiveType, type.getTypeSignature(), hiveColumnIndex, false));
            }
            hiveColumnIndex++;
        }

        // add the partition keys last (like Hive does)
        columns.addAll(getPartitionKeyColumnHandles(connectorId, table, hiveColumnIndex));

        return columns.build();
    }

    public static List<HiveColumnHandle> getPartitionKeyColumnHandles(String connectorId, Table table, int startOrdinal)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        for (int i = 0; i < partitionKeys.size(); i++) {
            FieldSchema field = partitionKeys.get(i);

            HiveType hiveType = getSupportedHiveType(field.getType());
            columns.add(new HiveColumnHandle(connectorId, field.getName(), startOrdinal + i, hiveType, getType(field.getType()).getTypeSignature(), -1, true));
        }

        return columns.build();
    }

    public static Slice base64Decode(byte[] bytes)
    {
        return Slices.wrappedBuffer(Base64.getDecoder().decode(bytes));
    }

    public static void checkCondition(boolean condition, ErrorCodeSupplier errorCode, String formatString, Object... args)
    {
        if (!condition) {
            throw new PrestoException(errorCode, format(formatString, args));
        }
    }
}
