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
import com.facebook.presto.common.function.SqlFunctionProperties;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.realCompare;
import static com.facebook.presto.common.type.TypeUtils.realEquals;
import static com.facebook.presto.common.type.TypeUtils.realHashCode;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class RealType
        extends AbstractIntType
{
    public static final RealType REAL = new RealType(true);
    public static final RealType OLD_NAN_REAL = new RealType(false);
    private final boolean useNewNanDefinition;

    private RealType(boolean useNewNanDefinition)
    {
        super(parseTypeSignature(StandardTypes.REAL));
        this.useNewNanDefinition = useNewNanDefinition;
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
        if (!useNewNanDefinition) {
            // direct equality is correct here
            // noinspection FloatingPointEquality
            return leftValue == rightValue;
        }
        return realEquals(leftValue, rightValue);
    }

    @Override
    public long hash(Block block, int position)
    {
        return realHashCode(intBitsToFloat(block.getInt(position)));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // WARNING: the correctness of InCodeGenerator is dependent on the implementation of this
        // function being the equivalence of internal long representation.
        float leftValue = intBitsToFloat(leftBlock.getInt(leftPosition));
        float rightValue = intBitsToFloat(rightBlock.getInt(rightPosition));
        return realCompare(leftValue, rightValue);
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
        return other instanceof RealType;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
