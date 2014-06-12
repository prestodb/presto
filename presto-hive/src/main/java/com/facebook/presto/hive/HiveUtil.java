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

import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.hive.HiveColumnHandle.isPartitionKeyPredicate;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;
import static com.google.common.io.BaseEncoding.base64;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.getTableMetadata;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public final class HiveUtil
{
    public static final String PRESTO_VIEW_FLAG = "presto_view";

    private static final String VIEW_PREFIX = "/* Presto View: ";
    private static final String VIEW_SUFFIX = " */";

    private static final DateTimeFormatter HIVE_TIMESTAMP_PARSER = new DateTimeFormatterBuilder()
            .append(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
            .appendOptional(DateTimeFormat.forPattern(".SSSSSSSSS").getParser())
            .toFormatter();

    private HiveUtil()
    {
    }

    private static FileSplit createFileSplit(final Path path, long start, long length)
    {
        return new FileSplit(path, start, length, (String[]) null)
        {
            @Override
            public Path getPath()
            {
                // make sure our original path object is returned
                return path;
            }
        };
    }

    public static RecordReader<?, ?> createRecordReader(String clientId, Configuration configuration, Path path, long start, long length, Properties schema, List<HiveColumnHandle> columns)
    {
        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, not(isPartitionKeyPredicate())));
        if (readColumns.isEmpty()) {
            // for count(*) queries we will have "no" columns we want to read, but since hive doesn't
            // support no columns (it will read all columns instead), we must choose a single column
            HiveColumnHandle primitiveColumn = getFirstPrimitiveColumn(clientId, schema);
            readColumns = ImmutableList.of(primitiveColumn);
        }
        ArrayList<Integer> readHiveColumnIndexes = new ArrayList<>(transform(readColumns, hiveColumnIndexGetter()));

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        ColumnProjectionUtils.setReadColumnIDs(configuration, readHiveColumnIndexes);
        configuration.set(IOConstants.COLUMNS, Joiner.on(',').join(Iterables.transform(readColumns, HiveColumnHandle.nameGetter())));

        final InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, true);
        final JobConf jobConf = new JobConf(configuration);
        final FileSplit fileSplit = createFileSplit(path, start, length);

        // propagate serialization configuration to getRecordReader
        for (String name : schema.stringPropertyNames()) {
            if (name.startsWith("serialization.")) {
                jobConf.set(name, schema.getProperty(name));
            }
        }

        try {
            return retry().stopOnIllegalExceptions().run("createRecordReader", new Callable<RecordReader<?, ?>>()
            {
                @Override
                public RecordReader<?, ?> call()
                        throws IOException
                {
                    return inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
                }
            });
        }
        catch (Exception e) {
            throw new PrestoException(HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT.toErrorCode(), String.format("Error opening Hive split %s (offset=%s, length=%s) using %s: %s",
                    path,
                    start,
                    length,
                    getInputFormatName(schema),
                    e.getMessage()),
                    e);
        }
    }

    private static HiveColumnHandle getFirstPrimitiveColumn(String clientId, Properties schema)
    {
        int index = 0;
        for (StructField field : getTableObjectInspector(schema).getAllStructFieldRefs()) {
            if (field.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
                PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) field.getFieldObjectInspector();
                HiveType hiveType = HiveType.getSupportedHiveType(inspector.getPrimitiveCategory());
                return new HiveColumnHandle(clientId, field.getFieldName(), index, hiveType, index, false);
            }
            index++;
        }

        throw new IllegalStateException("Table doesn't have any PRIMITIVE columns");
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
        Class<?> clazz = conf.getClassByName(inputFormatName);
        // TODO: remove redundant cast to Object after IDEA-118533 is fixed
        return (Class<? extends InputFormat<?, ?>>) (Object) clazz.asSubclass(InputFormat.class);
    }

    static String getInputFormatName(Properties schema)
    {
        String name = schema.getProperty(FILE_INPUT_FORMAT);
        checkArgument(name != null, "missing property: %s", FILE_INPUT_FORMAT);
        return name;
    }

    static PrimitiveObjectInspector.PrimitiveCategory convertNativeHiveType(String type)
    {
        return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(type).primitiveCategory;
    }

    public static Function<ConnectorPartition, String> partitionIdGetter()
    {
        return new Function<ConnectorPartition, String>()
        {
            @Override
            public String apply(ConnectorPartition input)
            {
                return input.getPartitionId();
            }
        };
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
        checkArgument(name != null, "missing property: %s", SERIALIZATION_LIB);

        Deserializer deserializer = createDeserializer(getDeserializerClass(name));
        initializeDeserializer(deserializer, schema);
        return deserializer;
    }

    @SuppressWarnings("deprecation")
    private static Class<? extends Deserializer> getDeserializerClass(String name)
    {
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

    public static boolean isPrestoView(Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    public static String encodeViewData(String data)
    {
        return VIEW_PREFIX + base64().encode(data.getBytes(UTF_8)) + VIEW_SUFFIX;
    }

    public static String decodeViewData(String data)
    {
        checkArgument(data.startsWith(VIEW_PREFIX), "View data missing prefix: %s", data);
        checkArgument(data.endsWith(VIEW_SUFFIX), "View data missing suffix: %s", data);
        data = data.substring(VIEW_PREFIX.length());
        data = data.substring(0, data.length() - VIEW_SUFFIX.length());
        return new String(base64().decode(data), UTF_8);
    }
}
