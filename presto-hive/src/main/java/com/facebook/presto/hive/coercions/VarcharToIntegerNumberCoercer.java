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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;

public class VarcharToIntegerNumberCoercer
        extends AbstractCoercer
{
    private final long minValue;
    private final long maxValue;

    public VarcharToIntegerNumberCoercer(Type fromType, Type toType)
    {
        super(fromType, toType);

        if (toType.equals(TINYINT)) {
            minValue = Byte.MIN_VALUE;
            maxValue = Byte.MAX_VALUE;
        }
        else if (toType.equals(SMALLINT)) {
            minValue = Short.MIN_VALUE;
            maxValue = Short.MAX_VALUE;
        }
        else if (toType.equals(INTEGER)) {
            minValue = Integer.MIN_VALUE;
            maxValue = Integer.MAX_VALUE;
        }
        else if (toType.equals(BIGINT)) {
            minValue = Long.MIN_VALUE;
            maxValue = Long.MAX_VALUE;
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Could not create Coercer from from varchar to %s", toType));
        }
    }

    @Override
    protected void appendCoercedSlice(BlockBuilder blockBuilder, Slice slice)
    {
        try {
            long value = Long.parseLong(slice.toStringUtf8());
            if (minValue <= value && value <= maxValue) {
                toType.writeLong(blockBuilder, value);
            }
            else {
                blockBuilder.appendNull();
            }
        }
        catch (NumberFormatException e) {
            blockBuilder.appendNull();
        }
    }
}
