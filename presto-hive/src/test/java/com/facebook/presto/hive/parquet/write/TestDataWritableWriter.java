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
package com.facebook.presto.hive.parquet.write;

import com.facebook.airlift.log.Logger;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * This class is copied from org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriter
 * and extended to support empty arrays and maps (HIVE-13632).
 * Additionally, there is a support for arrays without an inner element layer and
 * support for maps where  MAP_KEY_VALUE is incorrectly used in place of MAP
 * for backward-compatibility rules testing (https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists)
 */
public class TestDataWritableWriter
{
    private static final Logger log = Logger.get(DataWritableWriter.class);
    private final RecordConsumer recordConsumer;
    private final GroupType schema;
    private final boolean singleLevelArray;

    public TestDataWritableWriter(final RecordConsumer recordConsumer, final GroupType schema, boolean singleLevelArray)
    {
        this.recordConsumer = recordConsumer;
        this.schema = schema;
        this.singleLevelArray = singleLevelArray;
    }

    /**
     * It writes all record values to the Parquet RecordConsumer.
     *
     * @param record Contains the record that are going to be written.
     */
    public void write(final ParquetHiveRecord record)
    {
        if (record != null) {
            recordConsumer.startMessage();
            try {
                writeGroupFields(record.getObject(), record.getObjectInspector(), schema);
            }
            catch (RuntimeException e) {
                String errorMessage = "Parquet record is malformed: " + e.getMessage();
                log.error(errorMessage, e);
                throw new RuntimeException(errorMessage, e);
            }
            recordConsumer.endMessage();
        }
    }

    /**
     * It writes all the fields contained inside a group to the RecordConsumer.
     *
     * @param value The list of values contained in the group.
     * @param inspector The object inspector used to get the correct value type.
     * @param type Type that contains information about the group schema.
     */
    private void writeGroupFields(final Object value, final StructObjectInspector inspector, final GroupType type)
    {
        if (value != null) {
            List<? extends StructField> fields = inspector.getAllStructFieldRefs();
            List<Object> fieldValuesList = inspector.getStructFieldsDataAsList(value);

            for (int i = 0; i < type.getFieldCount(); i++) {
                Type fieldType = type.getType(i);
                String fieldName = fieldType.getName();
                Object fieldValue = fieldValuesList.get(i);

                if (fieldValue != null) {
                    ObjectInspector fieldInspector = fields.get(i).getFieldObjectInspector();
                    recordConsumer.startField(fieldName, i);
                    writeValue(fieldValue, fieldInspector, fieldType);
                    recordConsumer.endField(fieldName, i);
                }
            }
        }
    }

    /**
     * It writes the field value to the Parquet RecordConsumer. It detects the field type, and calls
     * the correct write function.
     *
     * @param value The writable object that contains the value.
     * @param inspector The object inspector used to get the correct value type.
     * @param type Type that contains information about the type schema.
     */
    private void writeValue(final Object value, final ObjectInspector inspector, final Type type)
    {
        if (type.isPrimitive()) {
            checkInspectorCategory(inspector, ObjectInspector.Category.PRIMITIVE);
            writePrimitive(value, (PrimitiveObjectInspector) inspector);
        }
        else {
            GroupType groupType = type.asGroupType();
            OriginalType originalType = type.getOriginalType();

            if (originalType != null && originalType.equals(OriginalType.LIST)) {
                checkInspectorCategory(inspector, ObjectInspector.Category.LIST);
                if (singleLevelArray) {
                    writeSingleLevelArray(value, (ListObjectInspector) inspector, groupType);
                }
                else {
                    writeArray(value, (ListObjectInspector) inspector, groupType);
                }
            }
            else if (originalType != null && (originalType.equals(OriginalType.MAP) || originalType.equals(OriginalType.MAP_KEY_VALUE))) {
                checkInspectorCategory(inspector, ObjectInspector.Category.MAP);
                writeMap(value, (MapObjectInspector) inspector, groupType);
            }
            else {
                checkInspectorCategory(inspector, ObjectInspector.Category.STRUCT);
                writeGroup(value, (StructObjectInspector) inspector, groupType);
            }
        }
    }

    /**
     * Checks that an inspector matches the category indicated as a parameter.
     *
     * @param inspector The object inspector to check
     * @param category The category to match
     * @throws IllegalArgumentException if inspector does not match the category
     */
    private void checkInspectorCategory(ObjectInspector inspector, ObjectInspector.Category category)
    {
        if (!inspector.getCategory().equals(category)) {
            throw new IllegalArgumentException("Invalid data type: expected " + category
                    + " type, but found: " + inspector.getCategory());
        }
    }

    /**
     * It writes a group type and all its values to the Parquet RecordConsumer.
     * This is used only for optional and required groups.
     *
     * @param value Object that contains the group values.
     * @param inspector The object inspector used to get the correct value type.
     * @param type Type that contains information about the group schema.
     */
    private void writeGroup(final Object value, final StructObjectInspector inspector, final GroupType type)
    {
        recordConsumer.startGroup();
        writeGroupFields(value, inspector, type);
        recordConsumer.endGroup();
    }

    /**
     * It writes a list type and its array elements to the Parquet RecordConsumer.
     * This is called when the original type (LIST) is detected by writeValue()/
     * This function assumes the following schema:
     * optional group arrayCol (LIST) {
     * repeated group array {
     * optional TYPE array_element;
     * }
     * }
     *
     * @param value The object that contains the array values.
     * @param inspector The object inspector used to get the correct value type.
     * @param type Type that contains information about the group (LIST) schema.
     */
    private void writeArray(final Object value, final ListObjectInspector inspector, final GroupType type)
    {
        // Get the internal array structure
        GroupType repeatedType = type.getType(0).asGroupType();
        recordConsumer.startGroup();

        List<?> arrayValues = inspector.getList(value);
        if (!arrayValues.isEmpty()) {
            recordConsumer.startField(repeatedType.getName(), 0);
            ObjectInspector elementInspector = inspector.getListElementObjectInspector();

            Type elementType = repeatedType.getType(0);
            String elementName = elementType.getName();

            for (Object element : arrayValues) {
                recordConsumer.startGroup();
                if (element != null) {
                    recordConsumer.startField(elementName, 0);
                    writeValue(element, elementInspector, elementType);
                    recordConsumer.endField(elementName, 0);
                }
                recordConsumer.endGroup();
            }

            recordConsumer.endField(repeatedType.getName(), 0);
        }
        recordConsumer.endGroup();
    }

    private void writeSingleLevelArray(final Object value, final ListObjectInspector inspector, final GroupType type)
    {
        // Get the internal array structure
        Type elementType = type.getType(0);

        recordConsumer.startGroup();

        List<?> arrayValues = inspector.getList(value);
        if (!arrayValues.isEmpty()) {
            recordConsumer.startField(elementType.getName(), 0);
            ObjectInspector elementInspector = inspector.getListElementObjectInspector();

            for (Object element : arrayValues) {
                if (element == null) {
                    throw new IllegalArgumentException("Array elements are requires in given schema definition");
                }
                writeValue(element, elementInspector, elementType);
            }

            recordConsumer.endField(elementType.getName(), 0);
        }
        recordConsumer.endGroup();
    }

    /**
     * It writes a map type and its key-pair values to the Parquet RecordConsumer.
     * This is called when the original type (MAP) is detected by writeValue().
     * This function assumes the following schema:
     * optional group mapCol (MAP) {
     * repeated group map (MAP_KEY_VALUE) {
     * required TYPE key;
     * optional TYPE value;
     * }
     * }
     *
     * @param value The object that contains the map key-values.
     * @param inspector The object inspector used to get the correct value type.
     * @param type Type that contains information about the group (MAP) schema.
     */
    private void writeMap(final Object value, final MapObjectInspector inspector, final GroupType type)
    {
        // Get the internal map structure (MAP_KEY_VALUE)
        GroupType repeatedType = type.getType(0).asGroupType();

        recordConsumer.startGroup();
        Map<?, ?> mapValues = inspector.getMap(value);
        if (mapValues != null && mapValues.size() > 0) {
            recordConsumer.startField(repeatedType.getName(), 0);

            Type keyType = repeatedType.getType(0);
            String keyName = keyType.getName();
            ObjectInspector keyInspector = inspector.getMapKeyObjectInspector();

            Type valuetype = repeatedType.getType(1);
            String valueName = valuetype.getName();
            ObjectInspector valueInspector = inspector.getMapValueObjectInspector();

            for (Map.Entry<?, ?> keyValue : mapValues.entrySet()) {
                recordConsumer.startGroup();
                if (keyValue != null) {
                    // write key element
                    Object keyElement = keyValue.getKey();
                    recordConsumer.startField(keyName, 0);
                    writeValue(keyElement, keyInspector, keyType);
                    recordConsumer.endField(keyName, 0);

                    // write value element
                    Object valueElement = keyValue.getValue();
                    if (valueElement != null) {
                        recordConsumer.startField(valueName, 1);
                        writeValue(valueElement, valueInspector, valuetype);
                        recordConsumer.endField(valueName, 1);
                    }
                }
                recordConsumer.endGroup();
            }

            recordConsumer.endField(repeatedType.getName(), 0);
        }
        recordConsumer.endGroup();
    }

    /**
     * It writes the primitive value to the Parquet RecordConsumer.
     *
     * @param value The object that contains the primitive value.
     * @param inspector The object inspector used to get the correct value type.
     */
    private void writePrimitive(final Object value, final PrimitiveObjectInspector inspector)
    {
        if (value == null) {
            return;
        }

        switch (inspector.getPrimitiveCategory()) {
            case VOID:
                return;
            case DOUBLE:
                recordConsumer.addDouble(((DoubleObjectInspector) inspector).get(value));
                break;
            case BOOLEAN:
                recordConsumer.addBoolean(((BooleanObjectInspector) inspector).get(value));
                break;
            case FLOAT:
                recordConsumer.addFloat(((FloatObjectInspector) inspector).get(value));
                break;
            case BYTE:
                recordConsumer.addInteger(((ByteObjectInspector) inspector).get(value));
                break;
            case INT:
                recordConsumer.addInteger(((IntObjectInspector) inspector).get(value));
                break;
            case LONG:
                recordConsumer.addLong(((LongObjectInspector) inspector).get(value));
                break;
            case SHORT:
                recordConsumer.addInteger(((ShortObjectInspector) inspector).get(value));
                break;
            case STRING:
                String v = ((StringObjectInspector) inspector).getPrimitiveJavaObject(value);
                recordConsumer.addBinary(Binary.fromString(v));
                break;
            case CHAR:
                String vChar = ((HiveCharObjectInspector) inspector).getPrimitiveJavaObject(value).getStrippedValue();
                recordConsumer.addBinary(Binary.fromString(vChar));
                break;
            case VARCHAR:
                String vVarchar = ((HiveVarcharObjectInspector) inspector).getPrimitiveJavaObject(value).getValue();
                recordConsumer.addBinary(Binary.fromString(vVarchar));
                break;
            case BINARY:
                byte[] vBinary = ((BinaryObjectInspector) inspector).getPrimitiveJavaObject(value);
                recordConsumer.addBinary(Binary.fromByteArray(vBinary));
                break;
            case TIMESTAMP:
                Timestamp ts = ((TimestampObjectInspector) inspector).getPrimitiveJavaObject(value);
                recordConsumer.addBinary(NanoTimeUtils.getNanoTime(ts, false).toBinary());
                break;
            case DECIMAL:
                HiveDecimal vDecimal = ((HiveDecimal) inspector.getPrimitiveJavaObject(value));
                DecimalTypeInfo decTypeInfo = (DecimalTypeInfo) inspector.getTypeInfo();
                recordConsumer.addBinary(decimalToBinary(vDecimal, decTypeInfo));
                break;
            case DATE:
                Date vDate = ((DateObjectInspector) inspector).getPrimitiveJavaObject(value);
                recordConsumer.addInteger(DateWritable.dateToDays(vDate));
                break;
            default:
                throw new IllegalArgumentException("Unsupported primitive data type: " + inspector.getPrimitiveCategory());
        }
    }

    private Binary decimalToBinary(final HiveDecimal hiveDecimal, final DecimalTypeInfo decimalTypeInfo)
    {
        int prec = decimalTypeInfo.precision();
        int scale = decimalTypeInfo.scale();
        byte[] decimalBytes = hiveDecimal.setScale(scale).unscaledValue().toByteArray();

        // Estimated number of bytes needed.
        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }

        System.arraycopy(decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length); // Padding leading zeroes/ones.
        return Binary.fromByteArray(tgt);
    }
}
