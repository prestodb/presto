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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.BlockIndex;
import com.facebook.presto.common.function.BlockPosition;
import com.facebook.presto.common.function.ScalarOperator;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.function.OperatorType.COMPARISON;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Collections.singletonList;

public final class VarcharType
        extends AbstractVarcharType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(VarcharType.class, lookup(), Slice.class);

    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    public static VarcharType createUnboundedVarcharType()
    {
        return VARCHAR;
    }

    public static VarcharType createVarcharType(int length)
    {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return new VarcharType(length);
    }

    public static TypeSignature getParametrizedVarcharSignature(String param)
    {
        return new TypeSignature(StandardTypes.VARCHAR, TypeSignatureParameter.of(param));
    }

    private VarcharType(int length)
    {
        super(
                length,
                new TypeSignature(
                        StandardTypes.VARCHAR,
                        singletonList(TypeSignatureParameter.of((long) length))));
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        int leftLength = leftBlock.getSliceLength(leftPosition);
        int rightLength = rightBlock.getSliceLength(rightPosition);
        if (leftLength != rightLength) {
            return false;
        }
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, leftLength);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }

    @ScalarOperator(COMPARISON)
    private static long comparisonOperator(Slice left, Slice right)
    {
        return left.compareTo(right);
    }

    @ScalarOperator(COMPARISON)
    private static long comparisonOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        int leftLength = leftBlock.getSliceLength(leftPosition);
        int rightLength = rightBlock.getSliceLength(rightPosition);
        return leftBlock.compareTo(leftPosition, 0, leftLength, rightBlock, rightPosition, 0, rightLength);
    }
}
