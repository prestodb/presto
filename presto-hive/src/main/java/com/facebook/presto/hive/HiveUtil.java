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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
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

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.bucketColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.isBucketColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.isPathColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.pathColumnHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_SERDE_NOT_FOUND;
import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.trimSpaces;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;
import static java.lang.Byte.parseByte;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Short.parseShort;
import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_UNNECESSARY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
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

    private static final Pattern SUPPORTED_DECIMAL_TYPE = Pattern.compile(DECIMAL_TYPE_NAME + "\\((\\d+),(\\d+)\\)");
    private static final int DECIMAL_PRECISION_GROUP = 1;
    private static final int DECIMAL_SCALE_GROUP = 2;

    private static final String BIG_DECIMAL_POSTFIX = "BD";

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

    public static RecordReader<?, ?> createRecordReader(Configuration configuration, Path path, long start, long length, Properties schema, List<HiveColumnHandle> columns)
    {
        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, column -> column.getColumnType() == REGULAR));
        List<Integer> readHiveColumnIndexes = ImmutableList.copyOf(transform(readColumns, HiveColumnHandle::getHiveColumnIndex));

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        setReadColumns(configuration, readHiveColumnIndexes);

        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, true);
        JobConf jobConf = new JobConf(configuration);
        FileSplit fileSplit = new FileSplit(path, start, length, (String[]) null);

        // propagate serialization configuration to getRecordReader
        schema.stringPropertyNames().stream()
                .filter(name -> name.startsWith("serialization."))
                .forEach(name -> jobConf.set(name, schema.getProperty(name)));

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
        // ORC uses a custom InputFormat but is always splittable
        if (inputFormat.getClass().getSimpleName().equals("OrcInputFormat")) {
            return true;
        }

        // use reflection to get isSplittable method on FileInputFormat
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

    public static StructObjectInspector getTableObjectInspector(@SuppressWarnings("deprecation") Deserializer deserializer)
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
        return getTableObjectInspector(getHiveSchema(table)).getAllStructFieldRefs();
    }

    public static boolean isDeserializerClass(Properties schema, Class<?> deserializerClass)
    {
        return getDeserializerClassName(schema).equals(deserializerClass.getName());
    }

    public static String getDeserializerClassName(Properties schema)
    {
        String name = schema.getProperty(SERIALIZATION_LIB);
        checkCondition(name != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive deserializer property: %s", SERIALIZATION_LIB);
        return name;
    }

    @SuppressWarnings("deprecation")
    public static Deserializer getDeserializer(Properties schema)
    {
        String name = getDeserializerClassName(schema);

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
            throw new PrestoException(HIVE_SERDE_NOT_FOUND, "deserializer does not exist: " + name);
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
            deserializer.initialize(new Configuration(false), schema);
        }
        catch (SerDeException e) {
            throw new RuntimeException("error initializing deserializer: " + deserializer.getClass().getName());
        }
    }

    public static boolean isHiveNull(byte[] bytes)
    {
        return bytes.length == 2 && bytes[0] == '\\' && bytes[1] == 'N';
    }

    public static NullableValue parsePartitionValue(String partitionName, String value, Type type, DateTimeZone timeZone)
    {
        boolean isNull = HIVE_DEFAULT_DYNAMIC_PARTITION.equals(value);

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (isNull) {
                return NullableValue.asNull(decimalType);
            }
            if (decimalType.isShort()) {
                if (value.isEmpty()) {
                    return NullableValue.of(decimalType, 0L);
                }
                return NullableValue.of(decimalType, shortDecimalPartitionKey(value, decimalType, partitionName));
            }
            else {
                if (value.isEmpty()) {
                    return NullableValue.of(decimalType, Decimals.encodeUnscaledValue(BigInteger.ZERO));
                }
                return NullableValue.of(decimalType, longDecimalPartitionKey(value, decimalType, partitionName));
            }
        }

        if (BOOLEAN.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(BOOLEAN);
            }
            if (value.isEmpty()) {
                return NullableValue.of(BOOLEAN, false);
            }
            return NullableValue.of(BOOLEAN, booleanPartitionKey(value, partitionName));
        }

        if (TINYINT.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(TINYINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(TINYINT, 0L);
            }
            return NullableValue.of(TINYINT, tinyintPartitionKey(value, partitionName));
        }

        if (SMALLINT.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(SMALLINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(SMALLINT, 0L);
            }
            return NullableValue.of(SMALLINT, smallintPartitionKey(value, partitionName));
        }

        if (INTEGER.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(INTEGER);
            }
            if (value.isEmpty()) {
                return NullableValue.of(INTEGER, 0L);
            }
            return NullableValue.of(INTEGER, integerPartitionKey(value, partitionName));
        }

        if (BIGINT.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(BIGINT);
            }
            if (value.isEmpty()) {
                return NullableValue.of(BIGINT, 0L);
            }
            return NullableValue.of(BIGINT, bigintPartitionKey(value, partitionName));
        }

        if (DATE.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(DATE);
            }
            return NullableValue.of(DATE, datePartitionKey(value, partitionName));
        }

        if (TIMESTAMP.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(TIMESTAMP);
            }
            return NullableValue.of(TIMESTAMP, timestampPartitionKey(value, timeZone, partitionName));
        }

        if (REAL.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(REAL);
            }
            if (value.isEmpty()) {
                return NullableValue.of(REAL, (long) floatToRawIntBits(0.0f));
            }
            return NullableValue.of(REAL, floatPartitionKey(value, partitionName));
        }

        if (DOUBLE.equals(type)) {
            if (isNull) {
                return NullableValue.asNull(DOUBLE);
            }
            if (value.isEmpty()) {
                return NullableValue.of(DOUBLE, 0.0);
            }
            return NullableValue.of(DOUBLE, doublePartitionKey(value, partitionName));
        }

        if (type instanceof VarcharType) {
            if (isNull) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, varcharPartitionKey(value, partitionName, type));
        }

        if (isCharType(type)) {
            if (isNull) {
                return NullableValue.asNull(type);
            }
            return NullableValue.of(type, charPartitionKey(value, partitionName, type));
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported Type [%s] for partition: %s", type, partitionName));
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

    public static Optional<DecimalType> getDecimalType(HiveType hiveType)
    {
        return getDecimalType(hiveType.getHiveTypeName());
    }

    public static Optional<DecimalType> getDecimalType(String hiveTypeName)
    {
        Matcher matcher = SUPPORTED_DECIMAL_TYPE.matcher(hiveTypeName);
        if (matcher.matches()) {
            int precision = parseInt(matcher.group(DECIMAL_PRECISION_GROUP));
            int scale = parseInt(matcher.group(DECIMAL_SCALE_GROUP));
            return Optional.of(createDecimalType(precision, scale));
        }
        else {
            return Optional.empty();
        }
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

    public static boolean isStructuralType(Type type)
    {
        String baseName = type.getTypeSignature().getBase();
        return baseName.equals(StandardTypes.MAP) || baseName.equals(StandardTypes.ARRAY) || baseName.equals(StandardTypes.ROW);
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

    public static long integerPartitionKey(String value, String name)
    {
        try {
            return parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for INTEGER partition key: %s", value, name));
        }
    }

    public static long smallintPartitionKey(String value, String name)
    {
        try {
            return parseShort(value);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for SMALLINT partition key: %s", value, name));
        }
    }

    public static long tinyintPartitionKey(String value, String name)
    {
        try {
            return parseByte(value);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for TINYINT partition key: %s", value, name));
        }
    }

    public static long floatPartitionKey(String value, String name)
    {
        try {
            return floatToRawIntBits(parseFloat(value));
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for FLOAT partition key: %s", value, name));
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

    public static long shortDecimalPartitionKey(String value, DecimalType type, String name)
    {
        return decimalPartitionKey(value, type, name).unscaledValue().longValue();
    }

    public static Slice longDecimalPartitionKey(String value, DecimalType type, String name)
    {
        return Decimals.encodeUnscaledValue(decimalPartitionKey(value, type, name).unscaledValue());
    }

    private static BigDecimal decimalPartitionKey(String value, DecimalType type, String name)
    {
        try {
            if (value.endsWith(BIG_DECIMAL_POSTFIX)) {
                value = value.substring(0, value.length() - BIG_DECIMAL_POSTFIX.length());
            }

            BigDecimal decimal = new BigDecimal(value);
            decimal = decimal.setScale(type.getScale(), ROUND_UNNECESSARY);
            if (decimal.precision() > type.getPrecision()) {
                throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, type.toString(), name));
            }
            return decimal;
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, type.toString(), name));
        }
    }

    public static Slice varcharPartitionKey(String value, String name, Type columnType)
    {
        Slice partitionKey = Slices.utf8Slice(value);
        VarcharType varcharType = checkType(columnType, VarcharType.class, "columnType");
        if (SliceUtf8.countCodePoints(partitionKey) > varcharType.getLength()) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, columnType.toString(), name));
        }
        return partitionKey;
    }

    public static Slice charPartitionKey(String value, String name, Type columnType)
    {
        Slice partitionKey = trimSpaces(Slices.utf8Slice(value));
        CharType charType = checkType(columnType, CharType.class, "columnType");
        if (SliceUtf8.countCodePoints(partitionKey) > charType.getLength()) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, columnType.toString(), name));
        }
        return partitionKey;
    }

    public static SchemaTableName schemaTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, HiveTableHandle.class, "tableHandle").getSchemaTableName();
    }

    public static List<HiveColumnHandle> hiveColumnHandles(String connectorId, Table table)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        // add the data fields first
        columns.addAll(getRegularColumnHandles(connectorId, table));

        // add the partition keys last (like Hive does)
        columns.addAll(getPartitionKeyColumnHandles(connectorId, table));

        // add hidden columns
        columns.add(pathColumnHandle(connectorId));
        if (table.getStorage().getBucketProperty().isPresent()) {
            columns.add(bucketColumnHandle(connectorId));
        }

        return columns.build();
    }

    public static List<HiveColumnHandle> getRegularColumnHandles(String connectorId, Table table)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        int hiveColumnIndex = 0;
        for (Column field : table.getDataColumns()) {
            // ignore unsupported types rather than failing
            HiveType hiveType = field.getType();
            if (hiveType.isSupportedType()) {
                columns.add(new HiveColumnHandle(connectorId, field.getName(), hiveType, hiveType.getTypeSignature(), hiveColumnIndex, REGULAR, field.getComment()));
            }
            hiveColumnIndex++;
        }

        return columns.build();
    }

    public static List<HiveColumnHandle> getPartitionKeyColumnHandles(String connectorId, Table table)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        List<Column> partitionKeys = table.getPartitionColumns();
        for (Column field : partitionKeys) {
            HiveType hiveType = field.getType();
            if (!hiveType.isSupportedType()) {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            columns.add(new HiveColumnHandle(connectorId, field.getName(), hiveType, hiveType.getTypeSignature(), -1, PARTITION_KEY, field.getComment()));
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

    @Nullable
    public static String columnExtraInfo(boolean partitionKey)
    {
        return partitionKey ? "partition key" : null;
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

    public static String getPrefilledColumnValue(HiveColumnHandle columnHandle, HivePartitionKey partitionKey, Path path, OptionalInt bucketNumber)
    {
        if (partitionKey != null) {
            return partitionKey.getValue();
        }
        if (isPathColumnHandle(columnHandle)) {
            return path.toString();
        }
        if (isBucketColumnHandle(columnHandle)) {
            return String.valueOf(bucketNumber.getAsInt());
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported hidden column: " + columnHandle);
    }

    public static void closeWithSuppression(RecordCursor recordCursor, Throwable throwable)
    {
        requireNonNull(recordCursor, "recordCursor is null");
        requireNonNull(throwable, "throwable is null");
        try {
            recordCursor.close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }
}
