package io.prestosql.operator.scalar;
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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Failures.internalError;

public final class MapGenericEquality
{
    private MapGenericEquality() {}

    public interface EqualityPredicate
    {
        Boolean equals(int leftMapIndex, int rightMapIndex)
                throws Throwable;
    }

    public static Boolean genericEqual(
            Type keyType,
            Block leftBlock,
            Block rightBlock,
            EqualityPredicate predicate)
    {
        if (leftBlock.getPositionCount() != rightBlock.getPositionCount()) {
            return false;
        }

        SingleMapBlock leftSingleMapLeftBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightSingleMapBlock = (SingleMapBlock) rightBlock;

        boolean indeterminate = false;
        for (int position = 0; position < leftSingleMapLeftBlock.getPositionCount(); position += 2) {
            Object key = readNativeValue(keyType, leftBlock, position);
            int leftPosition = position + 1;
            int rightPosition = rightSingleMapBlock.seekKey(key);
            if (rightPosition == -1) {
                return false;
            }

            try {
                Boolean result = predicate.equals(leftPosition, rightPosition);
                if (result == null) {
                    indeterminate = true;
                }
                else if (!result) {
                    return false;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (indeterminate) {
            return null;
        }
        return true;
    }
}
