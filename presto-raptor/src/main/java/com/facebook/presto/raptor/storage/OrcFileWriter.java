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

import com.facebook.presto.raptor.util.SyncingFileSystem;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.type.DecimalType;
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
import org.apache.hadoop.hive.ql.io.orc.NullMemoryManager;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcWriterOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

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
import static com.facebook.presto.raptor.storage.StorageType.arrayOf;
import static com.facebook.presto.raptor.storage.StorageType.mapOf;
import static com.facebook.presto.raptor.util.Types.isArrayType;
import static com.facebook.presto.raptor.util.Types.isMapType;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.MAP;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getPrimitiveTypeInfo;

public class OrcFileWriter
        implements Closeable
{
    private static final Configuration CONFIGURATION = new Configuration();
    private static final Constructor<? extends RecordWriter> WRITER_CONSTRUCTOR = getOrcWriterConstructor();

    private final List<Type> columnTypes;

    private final OrcSerde serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object orcRow;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;

    public OrcFileWriter(List<Long> columnIds, List<Type> columnTypes, File target)
    {
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        List<StorageType> storageTypes = ImmutableList.copyOf(toStorageTypes(columnTypes));
        Iterable<String> hiveTypeNames = storageTypes.stream().map(StorageType::getHiveTypeName).collect(toList());
        List<String> columnNames = ImmutableList.copyOf(transform(columnIds, toStringFunction()));

        Properties properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(columnNames));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        serializer = createSerializer(CONFIGURATION, properties);
        recordWriter = createRecordWriter(new Path(target.toURI()), CONFIGURATION);

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
        if (closed) {
            return;
        }
        closed = true;

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

    private static RecordWriter createRecordWriter(Path target, Configuration conf)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(FileSystem.class.getClassLoader());
                FileSystem fileSystem = new SyncingFileSystem(CONFIGURATION)) {
            OrcFile.WriterOptions options = new OrcWriterOptions(conf)
                    .memory(new NullMemoryManager(conf))
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

    private static List<ObjectInspector> getJavaObjectInspectors(List<StorageType> types)
    {
        return types.stream()
                .map(StorageType::getHiveTypeName)
                .map(TypeInfoUtils::getTypeInfoFromTypeString)
                .map(OrcFileWriter::getJavaObjectInspector)
                .collect(toList());
    }

    private static ObjectInspector getJavaObjectInspector(TypeInfo typeInfo)
    {
        Category category = typeInfo.getCategory();
        if (category == PRIMITIVE) {
            return getPrimitiveJavaObjectInspector(getPrimitiveTypeInfo(typeInfo.getTypeName()));
        }
        if (category == LIST) {
            ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
            return getStandardListObjectInspector(getJavaObjectInspector(listTypeInfo.getListElementTypeInfo()));
        }
        if (category == MAP) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            return getStandardMapObjectInspector(
                    getJavaObjectInspector(mapTypeInfo.getMapKeyTypeInfo()),
                    getJavaObjectInspector(mapTypeInfo.getMapValueTypeInfo()));
        }
        throw new PrestoException(INTERNAL_ERROR, "Unhandled storage type: " + category);
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
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return StorageType.decimal(decimalType.getPrecision(), decimalType.getScale());
        }
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
            if (type instanceof VarcharType) {
                return StorageType.STRING;
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return StorageType.BYTES;
            }
        }
        if (isArrayType(type)) {
            return arrayOf(toStorageType(type.getTypeParameters().get(0)));
        }
        if (isMapType(type)) {
            return mapOf(toStorageType(type.getTypeParameters().get(0)), toStorageType(type.getTypeParameters().get(1)));
        }
        throw new PrestoException(NOT_SUPPORTED, "No storage type for type: " + type);
    }
}
