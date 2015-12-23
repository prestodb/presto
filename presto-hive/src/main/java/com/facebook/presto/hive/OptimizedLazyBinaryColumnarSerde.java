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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarStruct;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This is a copy of org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnSerDe that avoids
 * a call to StringObjectInspector.getPrimitiveJavaObject() in serialize() to check whether
 * the string is empty (it calls getPrimitiveWritableObject() instead). This improves CPU
 * efficiency by avoiding turning the underlying bytes into a java String.
 *
 *
 * LazyBinaryColumnarSerDe. This serde combines elements of columnar serde and lazybinary serde
 * to produce a serde which serializes columns into a BytesRefArrayWritable in a compact binary
 * format and which is deserialized in a lazy, i.e. on-demand fashion.
 *
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class OptimizedLazyBinaryColumnarSerde
        extends OptimizedColumnarSerDeBase
{
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;

    @Override
    public String toString()
    {
        return getClass().toString()
                + "["
                + columnNames
                + ":"
                + columnTypes + "]";
    }

    @Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException
    {
        LazySerDeParameters serdeParams = new LazySerDeParameters(conf, tbl, getClass().getName());

        columnNames = serdeParams.getColumnNames();
        columnTypes = serdeParams.getColumnTypes();

        cachedObjectInspector = LazyBinaryFactory.createColumnarStructInspector(
                columnNames, columnTypes);
        int size = columnTypes.size();
        List<Integer> notSkipIDs = new ArrayList<Integer>();
        if (conf == null || ColumnProjectionUtils.isReadAllColumns(conf)) {
            for (int i = 0; i < size; i++) {
                notSkipIDs.add(i);
            }
        }
        else {
            notSkipIDs = ColumnProjectionUtils.getReadColumnIDs(conf);
        }
        cachedLazyStruct = new LazyBinaryColumnarStruct(cachedObjectInspector, notSkipIDs);

        super.initialize(size);
    }

    static final byte[] INVALID_UTF__SINGLE_BYTE = {(byte) Integer.parseInt("10111111", 2)};

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector)
            throws SerDeException
    {
        if (objInspector.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + objInspector.getTypeName());
        }

        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);

        LazyBinarySerDe.BooleanRef warnedOnceNullMapKey = new LazyBinarySerDe.BooleanRef(false);
        serializeStream.reset();
        serializedSize = 0;
        int streamOffset = 0;
        // Serialize each field
        for (int i = 0; i < fields.size(); i++) {
            // Get the field objectInspector and the field object.
            ObjectInspector foi = fields.get(i).getFieldObjectInspector();
            Object f = (list == null ? null : list.get(i));
            //empty strings are marked by an invalid utf single byte sequence. A valid utf stream cannot
            //produce this sequence
            if ((f != null) && (foi.getCategory().equals(ObjectInspector.Category.PRIMITIVE))
                    && ((PrimitiveObjectInspector) foi).getPrimitiveCategory().equals(
                    PrimitiveObjectInspector.PrimitiveCategory.STRING)
                    && ((StringObjectInspector) foi).getPrimitiveWritableObject(f).getLength() == 0) {
                serializeStream.write(INVALID_UTF__SINGLE_BYTE, 0, 1);
            }
            else {
                LazyBinarySerDe.serialize(serializeStream, f, foi, true, warnedOnceNullMapKey);
            }
            field[i].set(serializeStream.getData(), streamOffset, serializeStream
                    .getLength()
                    - streamOffset);
            streamOffset = serializeStream.getLength();
        }
        serializedSize = serializeStream.getLength();
        lastOperationSerialize = true;
        lastOperationDeserialize = false;
        return serializeCache;
    }
}
