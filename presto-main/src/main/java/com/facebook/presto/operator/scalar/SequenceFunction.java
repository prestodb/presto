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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Ints;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.Failures.checkCondition;

public final class SequenceFunction
{
    private SequenceFunction() {}

    @Description("Sequence function to generate synthetic arrays")
    @ScalarFunction
    @SqlType("array(bigint)")
    public static Block sequence(@SqlType("bigint") long start, @SqlType("bigint") long end)
    {
        checkCondition(end >= start, INVALID_FUNCTION_ARGUMENT, "sequence end value should be greater than or equal to start value");
        int length = Ints.checkedCast(end - start + 1);
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), length);
        for (long i = start; i <= end; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        return blockBuilder.build();
    }
}
