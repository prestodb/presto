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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UnknownType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedType;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardListObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardMapObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaIntObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaShortObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaVoidObjectInspector;

public final class ObjectInspectors
{
    private ObjectInspectors()
    {
    }

    public static ObjectInspector create(Type type, TypeManager typeManager)
    {
        TypeSignature typeSignature = type.getTypeSignature();
        switch (typeSignature.getBase()) {
            case UnknownType.NAME:
                return javaVoidObjectInspector;
            case StandardTypes.BIGINT:
                return javaLongObjectInspector;
            case StandardTypes.INTEGER:
                return javaIntObjectInspector;
            case StandardTypes.SMALLINT:
                return javaShortObjectInspector;
            case StandardTypes.TINYINT:
                return javaByteObjectInspector;
            case StandardTypes.BOOLEAN:
                return javaBooleanObjectInspector;
            case StandardTypes.DATE:
                return javaDateObjectInspector;
            case StandardTypes.DECIMAL:
                DecimalType dt = PrestoTypes.createDecimalType(typeSignature);
                return new JavaHiveDecimalObjectInspector(
                        new DecimalTypeInfo(dt.getPrecision(), dt.getScale()));
            case StandardTypes.REAL:
                return javaFloatObjectInspector;
            case StandardTypes.DOUBLE:
                return javaDoubleObjectInspector;
            case StandardTypes.TIMESTAMP:
                return javaTimestampObjectInspector;
            case StandardTypes.VARBINARY:
                return javaByteArrayObjectInspector;
            case StandardTypes.VARCHAR:
                return javaStringObjectInspector;
            case StandardTypes.CHAR:
                return javaStringObjectInspector;
            case StandardTypes.ROW:
                if (type instanceof RowType) {
                    return createForRow(((RowType) type), typeManager);
                }
                break;
            case StandardTypes.ARRAY:
                if (type instanceof ArrayType) {
                    return createForArray(((ArrayType) type), typeManager);
                }
                break;
            case StandardTypes.MAP:
                if (type instanceof MapType) {
                    return createForMap(((MapType) type), typeManager);
                }
                break;
        }
        throw unsupportedType(type);
    }

    private static ObjectInspector createForRow(RowType rowType, TypeManager typeManager)
    {
        List<RowType.Field> fields = rowType.getFields();
        int numField = fields.size();
        List<String> fieldNames = new ArrayList<>(numField);
        List<ObjectInspector> fieldInspectors = new ArrayList<>(numField);
        List<String> comments = new ArrayList<>(numField);

        for (int i = 0; i < numField; i++) {
            RowType.Field field = fields.get(i);
            String fieldName = field.getName().orElse("col" + i);
            fieldNames.add(fieldName);
            fieldInspectors.add(ObjectInspectors.create(field.getType(), typeManager));
            comments.add(fieldName);
        }

        return getStandardStructObjectInspector(fieldNames, fieldInspectors, comments);
    }

    private static ObjectInspector createForMap(MapType mapType, TypeManager typeManager)
    {
        ObjectInspector keyInspector = ObjectInspectors.create(mapType.getKeyType(), typeManager);
        ObjectInspector valueInspector = ObjectInspectors.create(mapType.getValueType(), typeManager);
        return getStandardMapObjectInspector(keyInspector, valueInspector);
    }

    private static ObjectInspector createForArray(ArrayType arrayType, TypeManager typeManager)
    {
        return getStandardListObjectInspector(ObjectInspectors.create(arrayType.getElementType(), typeManager));
    }
}
