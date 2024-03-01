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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.UnknownType;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.common.type.HiveDecimal;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.CHAR;
import static com.facebook.presto.common.type.StandardTypes.DATE;
import static com.facebook.presto.common.type.StandardTypes.DECIMAL;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.common.type.StandardTypes.VARBINARY;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.functions.HiveFunctionErrorCode.unsupportedType;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public final class ObjectInputDecoders
{
    private ObjectInputDecoders() {}

    public static ObjectInputDecoder createDecoder(Type type, TypeManager typeManager)
    {
        String base = type.getTypeSignature().getBase();
        switch (base) {
            case UnknownType.NAME:
                return o -> o;
            case BIGINT:
                return o -> (Long) o;
            case INTEGER:
                return o -> ((Long) o).intValue();
            case SMALLINT:
                return o -> ((Long) o).shortValue();
            case TINYINT:
                return o -> ((Long) o).byteValue();
            case BOOLEAN:
                return o -> (Boolean) o;
            case DATE:
                return DateTimeUtils::createDate;
            case DECIMAL:
                if (Decimals.isShortDecimal(type)) {
                    final int scale = ((DecimalType) type).getScale();
                    return o -> HiveDecimal.create(BigInteger.valueOf((long) o), scale);
                }
                else if (Decimals.isLongDecimal(type)) {
                    final int scale = ((DecimalType) type).getScale();
                    return o -> HiveDecimal.create(Decimals.decodeUnscaledValue((Slice) o), scale);
                }
                break;
            case REAL:
                return o -> intBitsToFloat(((Number) o).intValue());
            case DOUBLE:
                return o -> ((Double) o);
            case TIMESTAMP:
                return o -> new Timestamp(((long) o));
            case VARBINARY:
                return o -> ((Slice) o).getBytes();
            case VARCHAR:
                return o -> ((Slice) o).toStringUtf8();
            case CHAR:
                return o -> ((Slice) o).toStringUtf8();
            case ROW:
                return RowObjectInputDecoder.create(((RowType) type), typeManager);
            case ARRAY:
                return ArrayObjectInputDecoder.create(((ArrayType) type), typeManager);
            case MAP:
                return MapObjectInputDecoder.create(((MapType) type), typeManager);
        }

        throw unsupportedType(type);
    }

    public static BlockInputDecoder createBlockInputDecoder(Type type, TypeManager typeManager)
    {
        return BlockInputDecoders.createBlockInputDecoder(ObjectInspectors.create(type, typeManager), type);
    }

    public static class RowObjectInputDecoder
            implements ObjectInputDecoder
    {
        private final List<BlockInputDecoder> fieldDecoders;

        private static RowObjectInputDecoder create(RowType type, TypeManager typeManager)
        {
            List<BlockInputDecoder> fieldDecoders = type
                    .getFields()
                    .stream()
                    .map(f -> createBlockInputDecoder(f.getType(), typeManager))
                    .collect(Collectors.toList());
            return new RowObjectInputDecoder(fieldDecoders);
        }

        private RowObjectInputDecoder(List<BlockInputDecoder> fieldDecoders)
        {
            this.fieldDecoders = requireNonNull(fieldDecoders, "fieldDecoders is null");
        }

        @Override
        public Object decode(Object object)
        {
            if (object == null) {
                return null;
            }
            Block block = (Block) object;
            List<Object> list = new ArrayList<>(fieldDecoders.size());
            int numField = fieldDecoders.size();
            for (int i = 0; i < numField; i++) {
                list.add(fieldDecoders.get(i).decode(block, i));
            }
            return list;
        }
    }

    public static class ArrayObjectInputDecoder
            implements ObjectInputDecoder
    {
        private final BlockInputDecoder elementDecoder;

        private static ArrayObjectInputDecoder create(ArrayType type, TypeManager typeManager)
        {
            return new ArrayObjectInputDecoder(createBlockInputDecoder(type.getElementType(), typeManager));
        }

        private ArrayObjectInputDecoder(BlockInputDecoder elementDecoder)
        {
            this.elementDecoder = requireNonNull(elementDecoder, "elementDecoder is null");
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Object decode(Object object)
        {
            if (object == null) {
                return null;
            }
            Block block = (Block) object;
            int count = block.getPositionCount();
            List list = new ArrayList(count);
            for (int i = 0; i < count; i++) {
                list.add(elementDecoder.decode(block, i));
            }
            return list;
        }
    }

    public static class MapObjectInputDecoder
            implements ObjectInputDecoder
    {
        private final BlockInputDecoder keyDecoder;
        private final BlockInputDecoder valueDecoder;

        private static MapObjectInputDecoder create(MapType type, TypeManager typeManager)
        {
            return new MapObjectInputDecoder(
                    createBlockInputDecoder(type.getKeyType(), typeManager),
                    createBlockInputDecoder(type.getValueType(), typeManager));
        }

        private MapObjectInputDecoder(BlockInputDecoder keyDecoder, BlockInputDecoder valueDecoder)
        {
            this.keyDecoder = requireNonNull(keyDecoder, "keyDecoder is null");
            this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public Object decode(Object object)
        {
            if (object == null) {
                return null;
            }
            Block block = (Block) object;
            Map map = new HashMap();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                Object key = keyDecoder.decode(block, i);
                if (key != null) {
                    Object value = valueDecoder.decode(block, i + 1);
                    map.put(key, value);
                }
            }
            return map;
        }
    }
}
