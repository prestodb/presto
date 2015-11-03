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

package com.facebook.presto.testing;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public final class TestEquality
{
    private TestEquality() {}

    public static boolean equalToForTests(Type type, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            Block leftMapBlock = leftBlock.getObject(leftPosition, Block.class);
            Block rightMapBlock = rightBlock.getObject(rightPosition, Block.class);

            if (leftMapBlock.getPositionCount() != rightMapBlock.getPositionCount()) {
                return false;
            }

            Map<KeyWrapper, Integer> wrappedLeftMap = new HashMap<>();
            for (int position = 0; position < leftMapBlock.getPositionCount(); position += 2) {
                wrappedLeftMap.put(new KeyWrapper(mapType.getKeyType(), leftMapBlock, position), position + 1);
            }

            for (int position = 0; position < rightMapBlock.getPositionCount(); position += 2) {
                KeyWrapper key = new KeyWrapper(mapType.getKeyType(), rightMapBlock, position);
                Integer leftValuePosition = wrappedLeftMap.get(key);
                if (leftValuePosition == null) {
                    return false;
                }
                int rightValuePosition = position + 1;

                if (leftMapBlock.isNull(leftValuePosition) && rightMapBlock.isNull(rightValuePosition)) {
                    return true;
                }
                else if (leftMapBlock.isNull(leftValuePosition) ^ rightMapBlock.isNull(rightValuePosition)) {
                    return false;
                }
                else if (!equalToForTests(mapType.getValueType(), leftMapBlock, leftValuePosition, rightMapBlock, rightValuePosition)) {
                    return false;
                }
            }
            return true;
        }
        else if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            Block leftArray = leftBlock.getObject(leftPosition, Block.class);
            Block rightArray = rightBlock.getObject(rightPosition, Block.class);

            if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
                return false;
            }

            for (int i = 0; i < leftArray.getPositionCount(); i++) {
                if (leftArray.isNull(i) && rightArray.isNull(i)) {
                    return true;
                }
                else if (leftArray.isNull(i) ^ leftArray.isNull(i)) {
                    return false;
                }
                else if (!equalToForTests(arrayType.getElementType(), leftArray, i, rightArray, i)) {
                    return false;
                }
            }

            return true;
        }
        else if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            Block leftRow = leftBlock.getObject(leftPosition, Block.class);
            Block rightRow = rightBlock.getObject(rightPosition, Block.class);

            for (int i = 0; i < leftRow.getPositionCount(); i++) {
                checkElementNotNull(leftRow.isNull(i), "ROW comparison not supported for fields with null elements");
                checkElementNotNull(rightRow.isNull(i), "ROW comparison not supported for fields with null elements");
                Type fieldType = rowType.getFields().get(i).getType();
                if (!fieldType.equalTo(leftRow, i, rightRow, i)) {
                    return false;
                }
            }

            return true;
        }
        else if (type instanceof DoubleType) {
            Double leftValue = (Double) type.getObjectValue(SESSION, leftBlock, leftPosition);
            Double rightValue = (Double) type.getObjectValue(SESSION, rightBlock, rightPosition);
            if (leftValue == null && rightValue == null) {
                return true;
            }
            else if (leftValue == null ^ rightValue == null) {
                return false;
            }
            else {
                // Tests depend on this behavior
                if (isNaN(leftValue) && isNaN(rightValue)) {
                    return true;
                }
                if (isInfinite(leftValue) && isInfinite(rightValue)) {
                    return true;
                }
                return Math.abs(leftValue - rightValue) <= 1e-10;
            }
        }
        else {
            return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
        }
    }

    private static final class KeyWrapper
    {
        private final Type type;
        private final Block block;
        private final int position;

        public KeyWrapper(Type type, Block block, int position)
        {
            this.type = type;
            this.block = block;
            this.position = position;
        }

        public Block getBlock()
        {
            return this.block;
        }

        public int getPosition()
        {
            return this.position;
        }

        @Override
        public int hashCode()
        {
            return type.hash(block, position);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || !getClass().equals(obj.getClass())) {
                return false;
            }
            KeyWrapper other = (KeyWrapper) obj;
            return type.equalTo(this.block, this.position, other.getBlock(), other.getPosition());
        }
    }
}
