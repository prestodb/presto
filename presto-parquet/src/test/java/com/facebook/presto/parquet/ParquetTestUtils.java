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

package com.facebook.presto.parquet;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getVarcharTypeInfo;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class ParquetTestUtils
{
    private ParquetTestUtils()
    {
    }

    static void writeParquetColumnHive(File file, String columnName, boolean nullable, Type type, Iterator<?> values)
            throws Exception
    {
        JobConf jobConf = new JobConf();

        // Set this config to get around the issue of LocalFileSystem not getting registered when running the benchmarks using
        // the standalone jar with all dependencies
        jobConf.set("fs.file.impl", LocalFileSystem.class.getCanonicalName());

        jobConf.setLong(ParquetOutputFormat.BLOCK_SIZE, new DataSize(256, MEGABYTE).toBytes());
        jobConf.setLong(ParquetOutputFormat.PAGE_SIZE, new DataSize(100, KILOBYTE).toBytes());
        jobConf.set(ParquetOutputFormat.COMPRESSION, "snappy");

        Properties properties = new Properties();
        properties.setProperty("columns", columnName);
        properties.setProperty("columns.types", getHiveType(type));

        RecordWriter recordWriter = createParquetWriter(nullable, new Path(file.getAbsolutePath()), jobConf, properties, true);

        List<ObjectInspector> objectInspectors = getRowObjectInspectors(type);
        SettableStructObjectInspector tableObjectInspector = getStandardStructObjectInspector(ImmutableList.of(columnName), objectInspectors);

        Object row = tableObjectInspector.create();

        StructField structField = tableObjectInspector.getStructFieldRef(columnName);

        Setter setter = getSetter(type, tableObjectInspector, row, structField);

        Serializer serializer = initializeSerializer(jobConf, properties);

        while (values.hasNext()) {
            Object value = values.next();
            if (value == null) {
                tableObjectInspector.setStructFieldData(row, structField, null);
            }
            else {
                setter.set(value);
            }
            recordWriter.write(serializer.serialize(row, tableObjectInspector));
        }

        recordWriter.close(false);
    }

    private static String getHiveType(Type type)
    {
        if (type.equals(BOOLEAN) || type.equals(BIGINT) || type.equals(SmallintType.SMALLINT) ||
                type.equals(TinyintType.TINYINT) || type.equals(DOUBLE)) {
            return type.getTypeSignature().toString();
        }

        if (type.equals(INTEGER)) {
            return "int";
        }

        if (type.equals(REAL)) {
            return "float";
        }

        if (type.equals(TIMESTAMP)) {
            return "timestamp";
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            int varcharLength = varcharType.getLength();
            return "varchar(" + varcharLength + ")";
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return format("decimal(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
        }

        if (type instanceof ArrayType) {
            return "array<" + getHiveType(((ArrayType) type).getElementType()) + ">";
        }

        if (type instanceof RowType) {
            return "struct<" + Joiner.on(",").join(((RowType) type).getFields().stream().map(t -> t.getName().get() + ":" + getHiveType(t.getType())).collect(toList())) + ">";
        }

        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Serializer initializeSerializer(Configuration conf, Properties properties)
    {
        try {
            Serializer result = ParquetHiveSerDe.class.getConstructor().newInstance();
            result.initialize(conf, properties);
            return result;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<ObjectInspector> getRowObjectInspectors(Type... types)
    {
        return Arrays.stream(types).map(t -> getObjectInspector(t)).collect(toList());
    }

    private static ObjectInspector getObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return writableBooleanObjectInspector;
        }

        if (type.equals(BIGINT)) {
            return writableLongObjectInspector;
        }

        if (type.equals(INTEGER)) {
            return writableIntObjectInspector;
        }

        if (type.equals(SmallintType.SMALLINT)) {
            return writableShortObjectInspector;
        }

        if (type.equals(TinyintType.TINYINT)) {
            return writableByteObjectInspector;
        }

        if (type.equals(DOUBLE)) {
            return writableDoubleObjectInspector;
        }

        if (type.equals(REAL)) {
            return writableFloatObjectInspector;
        }

        if (type.equals(TIMESTAMP)) {
            return writableTimestampObjectInspector;
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            int varcharLength = varcharType.getLength();
            return getPrimitiveWritableObjectInspector(getVarcharTypeInfo(varcharLength));
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveWritableObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }

        if (type instanceof ArrayType || type instanceof RowType) {
            return getJavaObjectInspector(type);
        }

        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static Setter getSetter(Type type, SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
    {
        if (type.equals(BOOLEAN)) {
            return new BooleanSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(BIGINT)) {
            return new BigIntSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(INTEGER)) {
            return new IntegerSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(SmallintType.SMALLINT)) {
            return new ShortSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(TinyintType.TINYINT)) {
            return new TinyIntSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(DOUBLE)) {
            return new DoubleSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(REAL)) {
            return new FloatSetter(tableObjectInspector, row, structField);
        }

        if (type.equals(TIMESTAMP)) {
            return new TimestampSetter(tableObjectInspector, row, structField);
        }

        if (type instanceof VarcharType) {
            return new VarcharSetter(tableObjectInspector, row, structField);
        }

        if (type instanceof DecimalType) {
            return new DecimalSetter(tableObjectInspector, row, structField);
        }

        if (type instanceof ArrayType) {
            return new ArrayFieldSetter(tableObjectInspector, row, structField, type.getTypeParameters().get(0));
        }

        if (type instanceof RowType) {
            return new RowFieldSetter(tableObjectInspector, row, structField, type.getTypeParameters());
        }

        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        else if (type.equals(BIGINT)) {
            return javaLongObjectInspector;
        }
        else if (type.equals(INTEGER)) {
            return javaIntObjectInspector;
        }
        else if (type.equals(SmallintType.SMALLINT)) {
            return javaShortObjectInspector;
        }
        else if (type.equals(TinyintType.TINYINT)) {
            return javaByteObjectInspector;
        }
        else if (type.equals(REAL)) {
            return javaFloatObjectInspector;
        }
        else if (type.equals(DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        else if (type instanceof VarcharType) {
            return writableStringObjectInspector;
        }
        else if (type instanceof CharType) {
            return writableHiveCharObjectInspector;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        else if (type.equals(DateType.DATE)) {
            return javaDateObjectInspector;
        }
        else if (type.equals(TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getPrimitiveJavaObjectInspector(new DecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale()));
        }
        else if (type instanceof ArrayType) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        else if (type instanceof RowType) {
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    type.getTypeSignature().getParameters().stream()
                            .map(parameter -> parameter.getNamedTypeSignature().getName().get())
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(ParquetTestUtils::getJavaObjectInspector)
                            .collect(toList()));
        }

        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static RecordWriter createParquetWriter(boolean nullable, Path target, JobConf conf, Properties properties, boolean compress)
            throws IOException
    {
        return new MapredParquetOutputFormat()
        {
            @Override
            public RecordWriter getHiveRecordWriter(JobConf jobConf, Path finalOutPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable progress)
                    throws IOException
            {
                String columnNameProperty = tableProperties.getProperty("columns");
                String columnTypeProperty = tableProperties.getProperty("columns.types");
                Object columnNames = Arrays.asList(columnNameProperty.split(","));
                ArrayList<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

                MessageType messageType = HiveSchemaConverter.convert((List) columnNames, columnTypes);

                if (!nullable) {
                    // Parquet writer in Hive by default writes all columns as NULLABLE. We want to change it to write NON-NULLABLE
                    // type by changing the MessageType (parquet schema type)
                    List<org.apache.parquet.schema.Type> types = messageType.getFields();
                    List<org.apache.parquet.schema.Type> newTypes = new ArrayList<>();

                    for (org.apache.parquet.schema.Type type : types) {
                        newTypes.add(convertToRequiredType(type));
                    }
                    messageType = new MessageType("hive_schema", newTypes);
                }

                DataWritableWriteSupport.setSchema(messageType, jobConf);

                return this.getParquerRecordWriterWrapper(this.realOutputFormat, jobConf, finalOutPath.toString(), progress, tableProperties);
            }
        }.getHiveRecordWriter(conf, target, Text.class, compress, properties, Reporter.NULL);
    }

    private static org.apache.parquet.schema.Type convertToRequiredType(org.apache.parquet.schema.Type type)
    {
        if (type instanceof GroupType) {
            GroupType groupType = (GroupType) type;
            List<org.apache.parquet.schema.Type> fields = groupType.getFields();
            List<org.apache.parquet.schema.Type> newFields = new ArrayList<>();
            for (org.apache.parquet.schema.Type field : fields) {
                newFields.add(convertToRequiredType(field));
            }
            return new GroupType(REPEATED, groupType.getName(), newFields);
        }
        else if (type instanceof PrimitiveType) {
            PrimitiveType primitiveType = (PrimitiveType) type;
            Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(primitiveType.getPrimitiveTypeName(), REQUIRED);

            if (primitiveType.getDecimalMetadata() != null) {
                builder = (Types.PrimitiveBuilder<PrimitiveType>) builder.scale(primitiveType.getDecimalMetadata().getScale())
                        .precision(primitiveType.getDecimalMetadata().getPrecision());
            }

            return builder.length(primitiveType.getTypeLength())
                    .named(primitiveType.getName())
                    .asPrimitiveType();
        }

        throw new UnsupportedOperationException();
    }

    public static Object getField(Type type, Object value)
    {
        if (value == null) {
            return null;
        }
        if (BOOLEAN.equals(type) || BIGINT.equals(type) || INTEGER.equals(type) || DOUBLE.equals(type) || type.equals(REAL)) {
            return value;
        }

        if (type.equals(TIMESTAMP)) {
            return new Timestamp((Long) value);
        }

        if (type instanceof VarcharType) {
            return new Text(((String) value).getBytes());
        }

        if (type instanceof ArrayType) {
            List<Object> valueList = (List<Object>) value;
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < valueList.size(); i++) {
                Object element = getField(((ArrayType) type).getElementType(), valueList.get(i));
                list.add(element);
            }

            return Collections.unmodifiableList(list);
        }

        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            List<Object> valueList = (List<Object>) value;
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < valueList.size(); i++) {
                Object element = getField(fieldTypes.get(i), valueList.get(i));
                row.add(element);
            }

            return Collections.unmodifiableList(row);
        }

        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private abstract static class Setter
    {
        protected final SettableStructObjectInspector tableObjectInspector;
        protected final Object row;
        protected final StructField structField;

        Setter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            this.tableObjectInspector = tableObjectInspector;
            this.row = row;
            this.structField = structField;
        }

        abstract void set(Object value);
    }

    private static class ArrayFieldSetter
            extends Setter
    {
        private final Type elementType;

        public ArrayFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, Type elementType)
        {
            super(rowInspector, row, field);
            this.elementType = requireNonNull(elementType, "elementType is null");
        }

        @Override
        public void set(Object value)
        {
            List<Object> valueList = (List<Object>) value;
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < valueList.size(); i++) {
                Object element = getField(elementType, valueList.get(i));
                list.add(element);
            }

            tableObjectInspector.setStructFieldData(row, structField, list);
        }
    }

    private static class RowFieldSetter
            extends Setter
    {
        private final List<Type> fieldTypes;

        public RowFieldSetter(SettableStructObjectInspector rowInspector, Object row, StructField field, List<Type> fieldTypes)
        {
            super(rowInspector, row, field);
            this.fieldTypes = ImmutableList.copyOf(fieldTypes);
        }

        @Override
        public void set(Object value)
        {
            List<Object> valueList = (List<Object>) value;
            List<Object> valueObjects = new ArrayList<>(fieldTypes.size());
            for (int i = 0; i < fieldTypes.size(); i++) {
                Object element = getField(fieldTypes.get(i), valueList.get(i));
                valueObjects.add(element);
            }

            tableObjectInspector.setStructFieldData(row, structField, valueObjects);
        }
    }

    private static class BooleanSetter
            extends Setter
    {
        private final BooleanWritable writable = new BooleanWritable();

        BooleanSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Boolean) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class TinyIntSetter
            extends Setter
    {
        private final ByteWritable writable = new ByteWritable();

        TinyIntSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Byte) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class ShortSetter
            extends Setter
    {
        private final ShortWritable writable = new ShortWritable();

        ShortSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Short) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class IntegerSetter
            extends Setter
    {
        private final IntWritable writable = new IntWritable();

        IntegerSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Integer) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class BigIntSetter
            extends Setter
    {
        private final LongWritable writable = new LongWritable();

        BigIntSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Long) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class FloatSetter
            extends Setter
    {
        private final FloatWritable writable = new FloatWritable();

        FloatSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Float) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class DoubleSetter
            extends Setter
    {
        private final DoubleWritable writable = new DoubleWritable();

        DoubleSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((Double) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class TimestampSetter
            extends Setter
    {
        private final TimestampWritable writable = new TimestampWritable();

        TimestampSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.setTime((Long) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class DecimalSetter
            extends Setter
    {
        private final HiveDecimalWritable writable = new HiveDecimalWritable();

        DecimalSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set(HiveDecimal.create(((SqlDecimal) value).toBigDecimal()));
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }

    private static class VarcharSetter
            extends Setter
    {
        private final HiveVarcharWritable writable = new HiveVarcharWritable();

        VarcharSetter(SettableStructObjectInspector tableObjectInspector, Object row, StructField structField)
        {
            super(tableObjectInspector, row, structField);
        }

        @Override
        void set(Object value)
        {
            writable.set((String) value);
            tableObjectInspector.setStructFieldData(row, structField, writable);
        }
    }
}
