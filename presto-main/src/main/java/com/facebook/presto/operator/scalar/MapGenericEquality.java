package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Failures.internalError;

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
            MethodHandle keyNativeHashCode,
            MethodHandle keyBlockNativeEquals,
            MethodHandle keyBlockHashCode,
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

            int rightPosition;
            try {
                rightPosition = rightSingleMapBlock.seekKey(key, keyNativeHashCode, keyBlockNativeEquals, keyBlockHashCode);
            }
            catch (NotSupportedException e) {
                throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
            }

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
