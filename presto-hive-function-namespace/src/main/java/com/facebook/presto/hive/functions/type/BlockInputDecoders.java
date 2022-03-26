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

package com.facebook.presto.hive.functions.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.Streams;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedType;
import static com.facebook.presto.hive.functions.type.DateTimeUtils.createDate;
import static com.facebook.presto.hive.functions.type.DecimalUtils.readHiveDecimal;
import static com.facebook.presto.hive.functions.type.HiveTypes.createHiveChar;
import static com.facebook.presto.hive.functions.type.HiveTypes.createHiveVarChar;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;

public final class BlockInputDecoders
{
    private BlockInputDecoders() {}

    public static BlockInputDecoder createBlockInputDecoder(ObjectInspector inspector, Type type)
    {
        if (inspector instanceof ConstantObjectInspector) {
            Object constant = ((ConstantObjectInspector) inspector).getWritableConstantValue();
            return (b, i) -> constant;
        }
        if (inspector instanceof PrimitiveObjectInspector) {
            return createForPrimitive(((PrimitiveObjectInspector) inspector), type);
        }
        else if (inspector instanceof StandardStructObjectInspector) {
            checkArgument(type instanceof RowType);
            return createForStruct(((StandardStructObjectInspector) inspector), ((RowType) type));
        }
        else if (inspector instanceof SettableStructObjectInspector) {
            return createForStruct(((SettableStructObjectInspector) inspector), ((RowType) type));
        }
        else if (inspector instanceof StructObjectInspector) {
            return createForStruct(((StructObjectInspector) inspector), ((RowType) type));
        }
        else if (inspector instanceof ListObjectInspector) {
            checkArgument(type instanceof ArrayType);
            return createForList(((ListObjectInspector) inspector), ((ArrayType) type));
        }
        else if (inspector instanceof MapObjectInspector) {
            checkArgument(type instanceof MapType);
            return createForMap(((MapObjectInspector) inspector), ((MapType) type));
        }
        throw unsupportedType(inspector);
    }

    private static BlockInputDecoder createForPrimitive(PrimitiveObjectInspector inspector, Type type)
    {
        boolean preferWritable = inspector.preferWritable();
        if (inspector instanceof StringObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new Text(type.getSlice(b, i).getBytes()) :
                    (b, i) -> b.isNull(i) ? null : type.getSlice(b, i).toStringUtf8();
        }
        else if (inspector instanceof IntObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new IntWritable(((int) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : ((int) type.getLong(b, i));
        }
        else if (inspector instanceof BooleanObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new BooleanWritable(type.getBoolean(b, i)) :
                    (b, i) -> b.isNull(i) ? null : type.getBoolean(b, i);
        }
        else if (inspector instanceof FloatObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new FloatWritable(intBitsToFloat((int) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : intBitsToFloat((int) type.getLong(b, i));
        }
        else if (inspector instanceof DoubleObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new DoubleWritable(type.getDouble(b, i)) :
                    (b, i) -> b.isNull(i) ? null : type.getDouble(b, i);
        }
        else if (inspector instanceof LongObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new LongWritable(type.getLong(b, i)) :
                    (b, i) -> b.isNull(i) ? null : type.getLong(b, i);
        }
        else if (inspector instanceof ShortObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new ShortWritable(((short) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : ((short) type.getLong(b, i));
        }
        else if (inspector instanceof ByteObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new ByteWritable(((byte) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : ((byte) type.getLong(b, i));
        }
        else if (inspector instanceof JavaHiveVarcharObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : createHiveVarChar(type.getSlice(b, i).toStringUtf8());
        }
        else if (inspector instanceof JavaHiveCharObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : createHiveChar(type.getSlice(b, i).toStringUtf8());
        }
        else if (inspector instanceof JavaHiveDecimalObjectInspector) {
            checkArgument(type instanceof DecimalType);
            return (b, i) -> b.isNull(i) ? null : readHiveDecimal(((DecimalType) type), b, i);
        }
        else if (inspector instanceof JavaDateObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : new Date(TimeUnit.DAYS.toMillis(type.getLong(b, i)));
        }
        else if (inspector instanceof JavaTimestampObjectInspector) {
            return (b, i) -> b.isNull(i) ? null : new Timestamp(type.getLong(b, i));
        }
        else if (inspector instanceof HiveDecimalObjectInspector) {
            checkArgument(type instanceof DecimalType);
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new HiveDecimalWritable(readHiveDecimal(((DecimalType) type), b, i)) :
                    (b, i) -> b.isNull(i) ? null : readHiveDecimal(((DecimalType) type), b, i);
        }
        else if (inspector instanceof BinaryObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new BytesWritable(type.getSlice(b, i).getBytes()) :
                    (b, i) -> b.isNull(i) ? null : type.getSlice(b, i).getBytes();
        }
        else if (inspector instanceof DateObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new DateWritable(((int) type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : createDate(((int) type.getLong(b, i)));
        }
        else if (inspector instanceof TimestampObjectInspector) {
            return preferWritable ?
                    (b, i) -> b.isNull(i) ? null : new TimestampWritable(new Timestamp(type.getLong(b, i))) :
                    (b, i) -> b.isNull(i) ? null : new Timestamp(type.getLong(b, i));
        }
        else if (inspector instanceof VoidObjectInspector) {
            return (b, i) -> null;
        }
        throw unsupportedType(inspector);
    }

    private static BlockInputDecoder createForStruct(StandardStructObjectInspector structInspector, RowType rowType)
    {
        List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();
        List<RowType.Field> rowFields = rowType.getFields();
        checkArgument(rowFields.size() == structFields.size());
        List<BlockInputDecoder> fieldDecoders = Streams.zip(
                structFields.stream(),
                rowFields.stream(),
                (sf, rf) -> createBlockInputDecoder(sf.getFieldObjectInspector(), rf.getType()))
                .collect(Collectors.toList());
        final int numFields = structFields.size();
        return (b, i) -> {
            if (b.isNull(i)) {
                return null;
            }
            Block row = b.getBlock(i);
            Object result = structInspector.create();
            for (int j = 0; j < numFields; j++) {
                structInspector.setStructFieldData(result,
                        structFields.get(j),
                        fieldDecoders.get(j).decode(row, j));
            }
            return result;
        };
    }

    /**
     * TODO: Remove this function? pretty similar to createForStruct(StandardStructObjectInspector, RowType)
     */
    private static BlockInputDecoder createForStruct(SettableStructObjectInspector structInspector, RowType rowType)
    {
        List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();
        List<RowType.Field> rowFields = rowType.getFields();
        checkArgument(rowFields.size() == structFields.size());
        List<BlockInputDecoder> fieldDecoders = Streams.zip(
                structFields.stream(),
                rowFields.stream(),
                (sf, rf) -> createBlockInputDecoder(sf.getFieldObjectInspector(), rf.getType()))
                .collect(Collectors.toList());
        final int numFields = structFields.size();
        return (b, i) -> {
            if (b.isNull(i)) {
                return null;
            }
            Block row = b.getBlock(i);
            Object result = structInspector.create();
            for (int j = 0; j < numFields; j++) {
                structInspector.setStructFieldData(result,
                        structFields.get(j),
                        fieldDecoders.get(j).decode(row, j));
            }
            return result;
        };
    }

    private static BlockInputDecoder createForStruct(StructObjectInspector structInspector, RowType rowType)
    {
        List<? extends StructField> structFields = structInspector.getAllStructFieldRefs();
        List<RowType.Field> rowFields = rowType.getFields();
        checkArgument(rowFields.size() == structFields.size());
        List<BlockInputDecoder> fieldDecoders = Streams.zip(
                structFields.stream(),
                rowFields.stream(),
                (sf, rf) -> createBlockInputDecoder(sf.getFieldObjectInspector(), rf.getType()))
                .collect(Collectors.toList());
        final int numFields = structFields.size();
        return (b, i) -> {
            if (b.isNull(i)) {
                return null;
            }
            Block row = b.getBlock(i);
            ArrayList<Object> result = new ArrayList<>(numFields);
            for (int j = 0; j < numFields; j++) {
                result.add(fieldDecoders.get(j).decode(row, j));
            }
            return result;
        };
    }

    private static BlockInputDecoder createForList(ListObjectInspector inspector, ArrayType arrayType)
    {
        Type elementType = arrayType.getElementType();
        ObjectInspector elementInspector = inspector.getListElementObjectInspector();
        BlockInputDecoder decoder = createBlockInputDecoder(elementInspector, elementType);
        return (b, i) -> {
            if (b.isNull(i)) {
                return null;
            }
            Block array = b.getBlock(i);
            int positions = array.getPositionCount();
            ArrayList<Object> result = new ArrayList<>(positions);
            for (int j = 0; j < positions; j++) {
                result.add(decoder.decode(array, j));
            }
            return result;
        };
    }

    private static BlockInputDecoder createForMap(MapObjectInspector inspector, MapType mapType)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();
        ObjectInspector valueInspector = inspector.getMapValueObjectInspector();
        BlockInputDecoder keyDecoder = createBlockInputDecoder(keyInspector, keyType);
        BlockInputDecoder valueDecoder = createBlockInputDecoder(valueInspector, valueType);
        return (b, i) -> {
            if (b.isNull(i)) {
                return null;
            }
            SingleMapBlock single = (SingleMapBlock) b.getBlock(i);
            int positions = single.getPositionCount();
            HashMap<Object, Object> result = new HashMap<>();
            for (int j = 0; j < positions; j += 2) {
                Object key = keyDecoder.decode(single, j);
                Object value = valueDecoder.decode(single, j + 1);
                if (key != null) {
                    result.put(key, value);
                }
            }
            return result;
        };
    }
}
