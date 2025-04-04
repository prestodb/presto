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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;

public final class ArrayFunctions
{
    private ArrayFunctions()
    {
    }

    @ScalarFunction(visibility = HIDDEN)
    @SqlType("array(unknown)")
    public static Block arrayConstructor()
    {
        BlockBuilder blockBuilder = new ArrayType(UNKNOWN).createBlockBuilder(null, 0);
        return blockBuilder.build();
    }
}
