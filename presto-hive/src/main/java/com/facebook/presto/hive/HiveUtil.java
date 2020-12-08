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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hadoop.TextLineLengthLimitExceededException;
import com.facebook.presto.hive.avro.PrestoAvroSerDe;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.pagefile.PageInputFormat;
import com.facebook.presto.hive.util.FooterAwareRecordReader;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Chars.isCharType;
import static com.facebook.presto.common.type.Chars.trimTrailingSpaces;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.MAX_PARTITION_KEY_COLUMN_INDEX;
import static com.facebook.presto.hive.HiveColumnHandle.bucketColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.isBucketColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.isPathColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.pathColumnHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_SERDE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TABLE_BUCKETING_IS_IGNORED;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveSessionProperties.isHudiMetadataVerificationEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isPreferMetadataToListHudiFiles;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.metastore.MetastoreUtil.checkCondition;
import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.facebook.presto.hive.util.ConfigurationUtils.toJobConf;
import static com.facebook.presto.hive.util.CustomSplitConversionUtils.recreateSplitWithCustomInfo;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
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
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.COLLECTION_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public final class HiveUtil
{
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");

    private static final String VIEW_PREFIX = "/* Presto View: ";
    private static final String VIEW_SUFFIX = " */";
    private static final String MATERIALIZED_VIEW_PREFIX = "/* Presto Materialized View: ";
    private static final String MATERIALIZED_VIEW_SUFFIX = " */";

    private static final DateTimeFormatter HIVE_DATE_PARSER = ISODateTimeFormat.date().withZoneUTC();
    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER;
    private static final Field COMPRESSION_CODECS_FIELD;

    private static final Pattern SUPPORTED_DECIMAL_TYPE = Pattern.compile(DECIMAL_TYPE_NAME + "\\((\\d+),(\\d+)\\)");
    private static final int DECIMAL_PRECISION_GROUP = 1;
    private static final int DECIMAL_SCALE_GROUP = 2;

    private static final String BIG_DECIMAL_POSTFIX = "BD";

    public static final String CUSTOM_FILE_SPLIT_CLASS_KEY = "custom_split_class";

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

        try {
            COMPRESSION_CODECS_FIELD = TextInputFormat.class.getDeclaredField("compressionCodecs");
            COMPRESSION_CODECS_FIELD.setAccessible(true);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private HiveUtil()
    {
    }

    public static RecordReader<?, ?> createRecordReader(Configuration configuration, Path path, long start, long length, Properties schema, List<HiveColumnHandle> columns, Map<String, String> customSplitInfo)
    {
        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, column -> column.getColumnType() == REGULAR));
        List<Integer> readHiveColumnIndexes = ImmutableList.copyOf(transform(readColumns, HiveColumnHandle::getHiveColumnIndex));

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        setReadColumns(configuration, readHiveColumnIndexes);

        // Only propagate serialization schema configs by default
        Predicate<String> schemaFilter = schemaProperty -> schemaProperty.startsWith("serialization.");

        InputFormat<?, ?> inputFormat = getInputFormat(configuration, getInputFormatName(schema), true);
        JobConf jobConf = toJobConf(configuration);
        FileSplit fileSplit = new FileSplit(path, start, length, (String[]) null);
        if (!customSplitInfo.isEmpty() && isHudiRealtimeSplit(customSplitInfo)) {
            fileSplit = recreateSplitWithCustomInfo(fileSplit, customSplitInfo);

            // Add additional column information for record reader
            List<String> readHiveColumnNames = ImmutableList.copyOf(transform(readColumns, HiveColumnHandle::getName));
            jobConf.set(READ_COLUMN_NAMES_CONF_STR, Joiner.on(',').join(readHiveColumnNames));

            // Remove filter when using customSplitInfo as the record reader requires complete schema configs
            schemaFilter = schemaProperty -> true;
        }

        schema.stringPropertyNames().stream()
            .filter(schemaFilter)
            .forEach(name -> jobConf.set(name, schema.getProperty(name)));

        // add Airlift LZO and LZOP to head of codecs list so as to not override existing entries
        List<String> codecs = newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(jobConf.get("io.compression.codecs", "")));
        if (!codecs.contains(LzoCodec.class.getName())) {
            codecs.add(0, LzoCodec.class.getName());
        }
        if (!codecs.contains(LzopCodec.class.getName())) {
            codecs.add(0, LzopCodec.class.getName());
        }
        jobConf.set("io.compression.codecs", codecs.stream().collect(joining(",")));

        try {
            RecordReader<WritableComparable, Writable> recordReader = (RecordReader<WritableComparable, Writable>) inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);

            int headerCount = getHeaderCount(schema);
            //  Only skip header rows when the split is at the beginning of the file
            if (start == 0 && headerCount > 0) {
                Utilities.skipHeader(recordReader, headerCount, recordReader.createKey(), recordReader.createValue());
            }

            int footerCount = getFooterCount(schema);
            if (footerCount > 0) {
                recordReader = new FooterAwareRecordReader<>(recordReader, footerCount, jobConf);
            }

            return recordReader;
        }
        catch (IOException e) {
            if (e instanceof TextLineLengthLimitExceededException) {
                throw new PrestoException(HIVE_BAD_DATA, "Line too long in text file: " + path, e);
            }

            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, format("Error opening Hive split %s (offset=%s, length=%s) using %s: %s",
                    path,
                    start,
                    length,
                    getInputFormatName(schema),
                    firstNonNull(e.getMessage(), e.getClass().getName())),
                    e);
        }
    }

    private static boolean isHudiRealtimeSplit(Map<String, String> customSplitInfo)
    {
        String customSplitClass = customSplitInfo.get(CUSTOM_FILE_SPLIT_CLASS_KEY);
        return HoodieRealtimeFileSplit.class.getName().equals(customSplitClass);
    }

    public static void setReadColumns(Configuration configuration, List<Integer> readHiveColumnIndexes)
    {
        configuration.set(READ_COLUMN_IDS_CONF_STR, Joiner.on(',').join(readHiveColumnIndexes));
        configuration.setBoolean(READ_ALL_COLUMNS, false);
    }

    public static Optional<CompressionCodec> getCompressionCodec(TextInputFormat inputFormat, Path file)
    {
        CompressionCodecFactory compressionCodecFactory;

        try {
            compressionCodecFactory = (CompressionCodecFactory) COMPRESSION_CODECS_FIELD.get(inputFormat);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to find compressionCodec for inputFormat: " + inputFormat.getClass().getName(), e);
        }

        if (compressionCodecFactory == null) {
            return Optional.empty();
        }

        return Optional.ofNullable(compressionCodecFactory.getCodec(file));
    }

    public static InputFormat<?, ?> getInputFormat(Configuration configuration, String inputFormatName, boolean symlinkTarget)
    {
        try {
            JobConf jobConf = toJobConf(configuration);

            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (ClassNotFoundException | RuntimeException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Unable to create input format " + inputFormatName, e);
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

        if (PageInputFormat.class.getSimpleName().equals(inputFormatName)) {
            return PageInputFormat.class;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }

    static String getInputFormatName(Properties schema)
    {
        String name = schema.getProperty(FILE_INPUT_FORMAT);
        checkCondition(name != null, HIVE_INVALID_METADATA, "Table or partition is missing Hive input format property: %s", FILE_INPUT_FORMAT);
        return name;
    }

    /**
     * Checks whether to use RecordReader from configured InputFormat for reading or not. It will be used only if
     * the InputFormat is using a custom FileSplit implementation, and has an annotation named
     * UseRecordReaderFromInputFormat to specifically indicate using RecordReader.
     *
     * @param configuration Hadoop configuration
     * @param storage Hive storage information
     * @param customSplitInfo Custom split information
     * @return Boolean indicating whether to use RecordReader or not.
     */
    public static boolean shouldUseRecordReaderFromInputFormat(Configuration configuration, Storage storage,
                                                               Map<String, String> customSplitInfo)
    {
        if (customSplitInfo != null && customSplitInfo.containsKey(HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY)) {
            InputFormat<?, ?> inputFormat = HiveUtil.getInputFormat(configuration,
                    storage.getStorageFormat().getInputFormat(), false);
            return Arrays.stream(inputFormat.getClass().getAnnotations())
                    .map(Annotation::annotationType)
                    .map(Class::getSimpleName)
                    .anyMatch(name -> name.equals("UseRecordReaderFromInputFormat"));
        }

        return false;
    }

    /**
     * Checks whether to use File Splitting logic from configured InputFormat or not. It checks for the presence of an
     * annotation named UseFileSplitsFromInputFormat on the InputFormat class. In addition, there are some additional
     * checks for Apache Hudi tables.
     * @param inputFormat InputFormat configured for the table
     * @param conf Hadoop Configuration
     * @param tablePath  Base path for the table
     * @return Boolean indicating whether to use File Splitting logic from Input Format or not.
     */
    static boolean shouldUseFileSplitsFromInputFormat(ConnectorSession session, InputFormat<?, ?> inputFormat,
                                                      Configuration conf, String tablePath)
    {
        boolean hasUseSplitsAnnotation = Arrays.stream(inputFormat.getClass().getAnnotations())
                .map(Annotation::annotationType)
                .map(Class::getSimpleName)
                .anyMatch(name -> name.equals("UseFileSplitsFromInputFormat"));
        return hasUseSplitsAnnotation && (!isHudiTable(inputFormat) ||
                shouldUseFileSplitsForHudi(session, inputFormat, conf, tablePath));
    }

    /**
     * This function does additional checks for Apache Hudi tables, to check if File Splitting logic should be used from
     * Hudi's input format. For Hudi, we need to use file splits logic in the following scenarios:
     * 1. If the table type is Merge on Read, which means the InputFormat should extend from HoodieRealtimeInputFormat
     * 2. If the table is created through Hudi BOOTSTRAP operation.
     *
     * @param inputFormat InputFormat configured for the table
     * @param conf Hadoop Configuration
     * @param tablePath Base path for the table
     * @return Boolean indicating whether to use File Splitting logic from Input Format or not.
     */
    static boolean shouldUseFileSplitsForHudi(ConnectorSession session, InputFormat<?, ?> inputFormat, Configuration conf,
                                              String tablePath)
    {
        // Set Hudi metadata based listing configurations to be used through input format
        conf.set(HoodieMetadataConfig.METADATA_ENABLE_PROP, String.valueOf(isPreferMetadataToListHudiFiles(session)));
        conf.set(HoodieMetadataConfig.METADATA_VALIDATE_PROP, String.valueOf(isHudiMetadataVerificationEnabled(session)));

        if (inputFormat instanceof HoodieParquetRealtimeInputFormat) {
            return true;
        }

        HoodieTableMetaClient hoodieTableMetaClient = new HoodieTableMetaClient(conf, tablePath);
        return hoodieTableMetaClient.getTableConfig().getBootstrapBasePath().isPresent();
    }

    /**
     * Checks whether a table is a Hudi table based on configured InputFormat.
     * @param inputFormat InputFormat configured for the table
     * @return Boolean indicating whether the table is a Hudi table or not.
     */
    static boolean isHudiTable(InputFormat<?, ?> inputFormat)
    {
        return inputFormat instanceof HoodieParquetInputFormat;
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

    public static boolean isSplittable(InputFormat<?, ?> inputFormat, FileSystem fileSystem, Path path)
    {
        if (inputFormat.getClass().getSimpleName().equals("OrcInputFormat") ||
                inputFormat.getClass().getSimpleName().equals("RCFileInputFormat")) {
            return true;
        }

        // use reflection to get isSplittable method on inputFormat
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
            throw new RuntimeException(e);
        }
    }

    public static StructObjectInspector getTableObjectInspector(Deserializer deserializer)
    {
        try {
            ObjectInspector inspector = deserializer.getObjectInspector();
            checkArgument(inspector.getCategory() == Category.STRUCT, "expected STRUCT: %s", inspector.getCategory());
            return (StructObjectInspector) inspector;
        }
        catch (SerDeException e) {
            throw new RuntimeException(e);
        }
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

    public static Deserializer getDeserializer(Configuration configuration, Properties schema)
    {
        String name = getDeserializerClassName(schema);

        // for collection delimiter, Hive 1.x, 2.x uses "colelction.delim" but Hive 3.x uses "collection.delim"
        // see also https://issues.apache.org/jira/browse/HIVE-16922
        if (name.equals("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")) {
            if (schema.containsKey("colelction.delim") && !schema.containsKey(COLLECTION_DELIM)) {
                schema.put(COLLECTION_DELIM, schema.getProperty("colelction.delim"));
            }
        }

        Deserializer deserializer = createDeserializer(getDeserializerClass(name));
        initializeDeserializer(configuration, deserializer, schema);
        return deserializer;
    }

    private static Class<? extends Deserializer> getDeserializerClass(String name)
    {
        // CDH uses different names for Parquet
        if ("parquet.hive.serde.ParquetHiveSerDe".equals(name)) {
            return ParquetHiveSerDe.class;
        }

        if ("org.apache.hadoop.hive.serde2.avro.AvroSerDe".equals(name)) {
            return PrestoAvroSerDe.class;
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

    private static Deserializer createDeserializer(Class<? extends Deserializer> clazz)
    {
        try {
            return clazz.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("error creating deserializer: " + clazz.getName(), e);
        }
    }

    private static void initializeDeserializer(Configuration configuration, Deserializer deserializer, Properties schema)
    {
        try {
            configuration = copy(configuration); // Some SerDes (e.g. Avro) modify passed configuration
            deserializer.initialize(configuration, schema);
            validate(deserializer);
        }
        catch (SerDeException | RuntimeException e) {
            throw new RuntimeException("error initializing deserializer: " + deserializer.getClass().getName(), e);
        }
    }

    private static void validate(Deserializer deserializer)
    {
        if (deserializer instanceof AbstractSerDe && !((AbstractSerDe) deserializer).getConfigurationErrors().isEmpty()) {
            throw new RuntimeException("There are configuration errors: " + ((AbstractSerDe) deserializer).getConfigurationErrors());
        }
    }

    public static boolean isHiveNull(byte[] bytes)
    {
        return bytes.length == 2 && bytes[0] == '\\' && bytes[1] == 'N';
    }

    public static void verifyPartitionTypeSupported(String partitionName, Type type)
    {
        if (!isValidPartitionType(type)) {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported type [%s] for partition: %s", type, partitionName));
        }
    }

    private static boolean isValidPartitionType(Type type)
    {
        return type instanceof DecimalType ||
                BOOLEAN.equals(type) ||
                TINYINT.equals(type) ||
                SMALLINT.equals(type) ||
                INTEGER.equals(type) ||
                BIGINT.equals(type) ||
                REAL.equals(type) ||
                DOUBLE.equals(type) ||
                DATE.equals(type) ||
                TIMESTAMP.equals(type) ||
                isVarcharType(type) ||
                isCharType(type);
    }

    public static NullableValue parsePartitionValue(String partitionName, String value, Type type, DateTimeZone timeZone)
    {
        verifyPartitionTypeSupported(partitionName, type);
        boolean isNull = HIVE_DEFAULT_DYNAMIC_PARTITION.equals(value) || isHiveNull(value.getBytes(UTF_8));

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

        if (isVarcharType(type)) {
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

        throw new VerifyException(format("Unhandled type [%s] for partition: %s", type, partitionName));
    }

    public static String encodeViewData(String data)
    {
        return encodeView(data, VIEW_PREFIX, VIEW_SUFFIX);
    }

    public static String decodeViewData(String data)
    {
        return decodeView(data, VIEW_PREFIX, VIEW_SUFFIX);
    }

    public static String encodeMaterializedViewData(String data)
    {
        return encodeView(data, MATERIALIZED_VIEW_PREFIX, MATERIALIZED_VIEW_SUFFIX);
    }

    public static String decodeMaterializedViewData(String data)
    {
        return decodeView(data, MATERIALIZED_VIEW_PREFIX, MATERIALIZED_VIEW_SUFFIX);
    }

    private static String encodeView(String data, String prefix, String suffix)
    {
        return prefix + Base64.getEncoder().encodeToString(data.getBytes(UTF_8)) + suffix;
    }

    private static String decodeView(String data, String prefix, String suffix)
    {
        checkCondition(data.startsWith(prefix), HIVE_INVALID_VIEW_DATA, "View data missing prefix: %s", data);
        checkCondition(data.endsWith(suffix), HIVE_INVALID_VIEW_DATA, "View data missing suffix: %s", data);
        data = data.substring(prefix.length());
        data = data.substring(0, data.length() - suffix.length());
        return new String(Base64.getDecoder().decode(data), UTF_8);
    }

    public static Optional<DecimalType> getDecimalType(HiveType hiveType)
    {
        return getDecimalType(hiveType.getHiveTypeName().toString());
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
        VarcharType varcharType = (VarcharType) columnType;
        if (SliceUtf8.countCodePoints(partitionKey) > varcharType.getLength()) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, columnType.toString(), name));
        }
        return partitionKey;
    }

    public static Slice charPartitionKey(String value, String name, Type columnType)
    {
        Slice partitionKey = trimTrailingSpaces(Slices.utf8Slice(value));
        CharType charType = (CharType) columnType;
        if (SliceUtf8.countCodePoints(partitionKey) > charType.getLength()) {
            throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, format("Invalid partition value '%s' for %s partition key: %s", value, columnType.toString(), name));
        }
        return partitionKey;
    }

    public static SchemaTableName schemaTableName(ConnectorTableHandle tableHandle)
    {
        return ((HiveTableHandle) tableHandle).getSchemaTableName();
    }

    public static List<HiveColumnHandle> hiveColumnHandles(Table table)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        // add the data fields first
        columns.addAll(getRegularColumnHandles(table));

        // add the partition keys last (like Hive does)
        columns.addAll(getPartitionKeyColumnHandles(table));

        // add hidden columns
        columns.add(pathColumnHandle());
        if (table.getStorage().getBucketProperty().isPresent()) {
            columns.add(bucketColumnHandle());
        }

        return columns.build();
    }

    public static List<HiveColumnHandle> getRegularColumnHandles(Table table)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        int hiveColumnIndex = 0;
        for (Column field : table.getDataColumns()) {
            // ignore unsupported types rather than failing
            HiveType hiveType = field.getType();
            if (hiveType.isSupportedType()) {
                columns.add(new HiveColumnHandle(field.getName(), hiveType, hiveType.getTypeSignature(), hiveColumnIndex, REGULAR, field.getComment(), Optional.empty()));
            }
            hiveColumnIndex++;
        }

        return columns.build();
    }

    public static List<HiveColumnHandle> getPartitionKeyColumnHandles(Table table)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();

        List<Column> partitionKeys = table.getPartitionColumns();
        int partitionColumnIndex = MAX_PARTITION_KEY_COLUMN_INDEX;
        for (Column field : partitionKeys) {
            HiveType hiveType = field.getType();
            if (!hiveType.isSupportedType()) {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            columns.add(new HiveColumnHandle(field.getName(), hiveType, hiveType.getTypeSignature(), partitionColumnIndex--, PARTITION_KEY, field.getComment(), Optional.empty()));
        }

        return columns.build();
    }

    @Nullable
    public static String columnExtraInfo(boolean partitionKey)
    {
        return partitionKey ? "partition key" : null;
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
            if (!bucketNumber.isPresent()) {
                throw new PrestoException(HIVE_TABLE_BUCKETING_IS_IGNORED, "Table bucketing is ignored. The virtual \"$bucket\" column cannot be referenced.");
            }
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

    public static List<HiveType> extractStructFieldTypes(HiveType hiveType)
    {
        return ((StructTypeInfo) hiveType.getTypeInfo()).getAllStructFieldTypeInfos().stream()
                .map(typeInfo -> HiveType.valueOf(typeInfo.getTypeName()))
                .collect(toImmutableList());
    }

    public static List<String> extractStructFieldNames(HiveType hiveType)
    {
        return ((StructTypeInfo) hiveType.getTypeInfo()).getAllStructFieldNames();
    }

    public static int getHeaderCount(Properties schema)
    {
        return getPositiveIntegerValue(schema, "skip.header.line.count", "0");
    }

    public static int getFooterCount(Properties schema)
    {
        return getPositiveIntegerValue(schema, "skip.footer.line.count", "0");
    }

    private static int getPositiveIntegerValue(Properties schema, String key, String defaultValue)
    {
        String value = schema.getProperty(key, defaultValue);
        try {
            int intValue = Integer.parseInt(value);
            if (intValue < 0) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Invalid value for %s property: %s", key, value));
            }
            return intValue;
        }
        catch (NumberFormatException e) {
            throw new PrestoException(HIVE_INVALID_METADATA, format("Invalid value for %s property: %s", key, value));
        }
    }

    public static Object typedPartitionKey(String value, Type type, String name, DateTimeZone hiveStorageTimeZone)
    {
        byte[] bytes = value.getBytes(UTF_8);

        if (isHiveNull(bytes)) {
            return null;
        }
        else if (type.equals(BOOLEAN)) {
            return booleanPartitionKey(value, name);
        }
        else if (type.equals(BIGINT)) {
            return bigintPartitionKey(value, name);
        }
        else if (type.equals(INTEGER)) {
            return integerPartitionKey(value, name);
        }
        else if (type.equals(SMALLINT)) {
            return smallintPartitionKey(value, name);
        }
        else if (type.equals(TINYINT)) {
            return tinyintPartitionKey(value, name);
        }
        else if (type.equals(REAL)) {
            return floatPartitionKey(value, name);
        }
        else if (type.equals(DOUBLE)) {
            return doublePartitionKey(value, name);
        }
        else if (isVarcharType(type)) {
            return varcharPartitionKey(value, name, type);
        }
        else if (isCharType(type)) {
            return charPartitionKey(value, name, type);
        }
        else if (type.equals(DATE)) {
            return datePartitionKey(value, name);
        }
        else if (type.equals(TIMESTAMP)) {
            return timestampPartitionKey(value, hiveStorageTimeZone, name);
        }
        else if (isShortDecimal(type)) {
            return shortDecimalPartitionKey(value, (DecimalType) type, name);
        }
        else if (isLongDecimal(type)) {
            return longDecimalPartitionKey(value, (DecimalType) type, name);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition column: %s", type.getDisplayName(), name));
        }
    }

    public static List<HiveColumnHandle> getPhysicalHiveColumnHandles(List<HiveColumnHandle> columns, boolean useOrcColumnNames, List<OrcType> types, Path path)
    {
        if (!useOrcColumnNames) {
            return columns;
        }

        List<String> columnNames = getColumnNames(types);
        verifyFileHasColumnNames(columnNames, path);

        Map<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMap(columnNames);
        int nextMissingColumnIndex = physicalNameOrdinalMap.size();

        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
            if (physicalOrdinal == null) {
                // if the column is missing from the file, assign it a column number larger than the number of columns in the
                // file so the reader will fill it with nulls.  If the index is negative, i.e. this is a sythesized column like
                // a partitioning key, $bucket or $path, leave it as is.
                if (column.getHiveColumnIndex() < 0) {
                    physicalOrdinal = column.getHiveColumnIndex();
                }
                else {
                    physicalOrdinal = nextMissingColumnIndex;
                    nextMissingColumnIndex++;
                }
            }
            physicalColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment(), column.getRequiredSubfields(), column.getPartialAggregation()));
        }
        return physicalColumns.build();
    }

    private static List<String> getColumnNames(List<OrcType> types)
    {
        return types.get(0).getFieldNames();
    }

    private static void verifyFileHasColumnNames(List<String> physicalColumnNames, Path path)
    {
        if (!physicalColumnNames.isEmpty() && physicalColumnNames.stream().allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    private static Map<String, Integer> buildPhysicalNameOrdinalMap(List<String> columnNames)
    {
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();

        int ordinal = 0;
        for (String physicalColumnName : columnNames) {
            physicalNameOrdinalMap.put(physicalColumnName, ordinal);
            ordinal++;
        }

        return physicalNameOrdinalMap.build();
    }

    /**
     * Translates Presto type that is incompatible (cannot be stored in a Hive table) to a compatible type with the same physical layout.
     * This allows to store more data types in a Hive temporary table than the Hive permanent tables support.
     */
    public static List<ColumnMetadata> translateHiveUnsupportedTypesForTemporaryTable(List<ColumnMetadata> columns, TypeManager typeManager)
    {
        return columns.stream()
                .map(column -> new ColumnMetadata(
                        column.getName(),
                        translateHiveUnsupportedTypeForTemporaryTable(column.getType(), typeManager),
                        column.isNullable(),
                        column.getComment(),
                        column.getExtraInfo(),
                        column.isHidden(),
                        column.getProperties()))
                .collect(toImmutableList());
    }

    public static Type translateHiveUnsupportedTypeForTemporaryTable(Type type, TypeManager typeManager)
    {
        return typeManager.getType(translateHiveUnsupportedTypeSignatureForTemporaryTable(type.getTypeSignature()));
    }

    private static TypeSignature translateHiveUnsupportedTypeSignatureForTemporaryTable(TypeSignature typeSignature)
    {
        List<TypeSignatureParameter> parameters = typeSignature.getParameters();

        if (typeSignature.getBase().equals("unknown")) {
            return new TypeSignature(StandardTypes.BOOLEAN);
        }

        if (typeSignature.getBase().equals(StandardTypes.ROW)) {
            ImmutableList.Builder<TypeSignatureParameter> updatedParameters = ImmutableList.builder();
            for (int i = 0; i < parameters.size(); i++) {
                TypeSignatureParameter typeSignatureParameter = parameters.get(i);
                checkArgument(typeSignatureParameter.isNamedTypeSignature(), "unexpected row type signature parameter: %s", typeSignatureParameter);
                NamedTypeSignature namedTypeSignature = typeSignatureParameter.getNamedTypeSignature();
                updatedParameters.add(TypeSignatureParameter.of(new NamedTypeSignature(
                        Optional.of(namedTypeSignature.getFieldName().orElse(new RowFieldName("_field_" + i, false))),
                        translateHiveUnsupportedTypeSignatureForTemporaryTable(namedTypeSignature.getTypeSignature()))));
            }
            return new TypeSignature(StandardTypes.ROW, updatedParameters.build());
        }

        if (!parameters.isEmpty()) {
            ImmutableList.Builder<TypeSignatureParameter> updatedParameters = ImmutableList.builder();
            for (TypeSignatureParameter parameter : parameters) {
                switch (parameter.getKind()) {
                    case LONG:
                    case VARIABLE:
                        updatedParameters.add(parameter);
                        continue;
                    case TYPE:
                        updatedParameters.add(TypeSignatureParameter.of(translateHiveUnsupportedTypeSignatureForTemporaryTable(parameter.getTypeSignature())));
                        break;
                    case NAMED_TYPE:
                        NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                        updatedParameters.add(TypeSignatureParameter.of(new NamedTypeSignature(
                                namedTypeSignature.getFieldName(),
                                translateHiveUnsupportedTypeSignatureForTemporaryTable(namedTypeSignature.getTypeSignature()))));
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected parameter type: " + parameter.getKind());
                }
            }
            return new TypeSignature(typeSignature.getBase(), updatedParameters.build());
        }

        return typeSignature;
    }
}
