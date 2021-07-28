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

import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.IsNull;
import com.facebook.presto.common.function.ScalarOperator;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.function.OperatorType.COMPARISON;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public final class RealType
        extends AbstractIntType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(RealType.class, lookup(), long.class);

    public static final RealType REAL = new RealType();

    private RealType()
    {
        super(parseTypeSignature(StandardTypes.REAL));
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return intBitsToFloat(block.getInt(position));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        float leftValue = intBitsToFloat(leftBlock.getInt(leftPosition));
        float rightValue = intBitsToFloat(rightBlock.getInt(rightPosition));

        // direct equality is correct here
        // noinspection FloatingPointEquality
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        // convert to canonical NaN if necessary
        return hash(floatToIntBits(intBitsToFloat(block.getInt(position))));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // WARNING: the correctness of InCodeGenerator is dependent on the implementation of this
        // function being the equivalence of internal long representation.
        float leftValue = intBitsToFloat(leftBlock.getInt(leftPosition));
        float rightValue = intBitsToFloat(rightBlock.getInt(rightPosition));
        return Float.compare(leftValue, rightValue);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        try {
            toIntExact(value);
        }
        catch (ArithmeticException e) {
            throw new GenericInternalException(format("Value (%sb) is not a valid single-precision float", Long.toBinaryString(value).replace(' ', '0')));
        }
        blockBuilder.writeInt((int) value).closeEntry();
    }

    @Override
    public boolean equals(Object other)
    {
        return other == REAL;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(long left, long right)
    {
        return intBitsToFloat((int) left) == intBitsToFloat((int) right);
    }

    @ScalarOperator(HASH_CODE)
    private static long hashCodeOperator(long value)
    {
        float realValue = intBitsToFloat((int) value);
        if (realValue == 0) {
            realValue = 0;
        }
        return AbstractLongType.hash(floatToIntBits(realValue));
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(long value)
    {
        float realValue = intBitsToFloat((int) value);
        if (realValue == 0) {
            realValue = 0;
        }
        return XxHash64.hash(floatToIntBits(realValue));
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    private static boolean distinctFromOperator(long left, @IsNull boolean leftNull, long right, @IsNull boolean rightNull)
    {
        if (leftNull || rightNull) {
            return leftNull != rightNull;
        }

        float leftFloat = intBitsToFloat((int) left);
        float rightFloat = intBitsToFloat((int) right);
        if (Float.isNaN(leftFloat) && Float.isNaN(rightFloat)) {
            return false;
        }
        return leftFloat != rightFloat;
    }

    @ScalarOperator(COMPARISON)
    private static long comparisonOperator(long left, long right)
    {
        return Float.compare(intBitsToFloat((int) left), intBitsToFloat((int) right));
    }

    @ScalarOperator(LESS_THAN)
    private static boolean lessThanOperator(long left, long right)
    {
        return intBitsToFloat((int) left) < intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    private static boolean lessThanOrEqualOperator(long left, long right)
    {
        return intBitsToFloat((int) left) <= intBitsToFloat((int) right);
    }
}
