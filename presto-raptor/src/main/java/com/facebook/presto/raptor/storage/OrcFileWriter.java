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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.storage.Row.extractRow;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public class OrcFileWriter
        implements Closeable
{
    private static final JobConf JOB_CONF = createJobConf();
    private static final Constructor<? extends RecordWriter> WRITER_CONSTRUCTOR = getOrcWriterConstructor();

    private final List<Type> columnTypes;

    private final OrcSerde serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object orcRow;

    private long rowCount;
    private long uncompressedSize;

    public OrcFileWriter(List<Long> columnIds, List<Type> columnTypes, File target)
    {
        this.columnTypes = ImmutableList.copyOf(checkNotNull(columnTypes, "columnTypes is null"));
        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        List<StorageType> storageTypes = ImmutableList.copyOf(toStorageTypes(columnTypes));
        Iterable<String> hiveTypeNames = storageTypes.stream().map(StorageType::getHiveTypeName).collect(toList());
        List<String> columnNames = ImmutableList.copyOf(transform(columnIds, toStringFunction()));

        Properties properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(columnNames));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        serializer = createSerializer(JOB_CONF, properties);
        recordWriter = createRecordWriter(new Path(target.toURI()), JOB_CONF);

        tableInspector = getStandardStructObjectInspector(columnNames, getJavaObjectInspectors(storageTypes));
        structFields = ImmutableList.copyOf(tableInspector.getAllStructFieldRefs());
        orcRow = tableInspector.create();
    }

    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                appendRow(extractRow(page, position, columnTypes));
            }
        }
    }

    public void appendPages(List<Page> inputPages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");
        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = inputPages.get(pageIndexes[i]);
            appendRow(extractRow(page, positionIndexes[i], columnTypes));
        }
    }

    public void appendRow(Row row)
    {
        List<Object> columns = row.getColumns();
        checkArgument(columns.size() == columnTypes.size());
        for (int channel = 0; channel < columns.size(); channel++) {
            tableInspector.setStructFieldData(orcRow, structFields.get(channel), columns.get(channel));
        }
        try {
            recordWriter.write(serializer.serialize(orcRow, tableInspector));
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to write record", e);
        }
        rowCount++;
        uncompressedSize += row.getSizeInBytes();
    }

    @Override
    public void close()
    {
        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    private static OrcSerde createSerializer(Configuration conf, Properties properties)
    {
        OrcSerde serde = new OrcSerde();
        serde.initialize(conf, properties);
        return serde;
    }

    private static RecordWriter createRecordWriter(Path target, JobConf conf)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader());
                FileSystem fileSystem = new SyncingFileSystem()) {
            OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
                    .fileSystem(fileSystem)
                    .compress(SNAPPY);
            return WRITER_CONSTRUCTOR.newInstance(target, options);
        }
        catch (ReflectiveOperationException | IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to create writer", e);
        }
    }

    private static Constructor<? extends RecordWriter> getOrcWriterConstructor()
    {
        try {
            String writerClassName = OrcOutputFormat.class.getName() + "$OrcRecordWriter";
            Constructor<? extends RecordWriter> constructor = OrcOutputFormat.class.getClassLoader()
                    .loadClass(writerClassName).asSubclass(RecordWriter.class)
                    .getDeclaredConstructor(Path.class, OrcFile.WriterOptions.class);
            constructor.setAccessible(true);
            return constructor;
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private static JobConf createJobConf()
    {
        JobConf jobConf = new JobConf();
        jobConf.setClassLoader(JobConf.class.getClassLoader());
        return new JobConf();
    }

    private static List<ObjectInspector> getJavaObjectInspectors(List<StorageType> types)
    {
        return types.stream().map(OrcFileWriter::getJavaObjectInspector).collect(toList());
    }

    private static ObjectInspector getJavaObjectInspector(StorageType type)
    {
        switch (type) {
            case BOOLEAN:
                return javaBooleanObjectInspector;
            case LONG:
                return javaLongObjectInspector;
            case DOUBLE:
                return javaDoubleObjectInspector;
            case STRING:
                return javaStringObjectInspector;
            case BYTES:
                return javaByteArrayObjectInspector;
        }
        throw new PrestoException(INTERNAL_ERROR, "Unhandled storage type: " + type);
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }

    private static List<StorageType> toStorageTypes(List<Type> columnTypes)
    {
        return columnTypes.stream().map(OrcFileWriter::toStorageType).collect(toList());
    }

    private static StorageType toStorageType(Type type)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return StorageType.BOOLEAN;
        }
        if (javaType == long.class) {
            return StorageType.LONG;
        }
        if (javaType == double.class) {
            return StorageType.DOUBLE;
        }
        if (javaType == Slice.class) {
            if (type.equals(VarcharType.VARCHAR)) {
                return StorageType.STRING;
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return StorageType.BYTES;
            }
        }
        throw new PrestoException(NOT_SUPPORTED, "No storage type for type: " + type);
    }
}
