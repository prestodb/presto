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

package com.facebook.presto.type.setdigest;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

@AggregationFunction("make_set_digest")
public final class BuildSetDigestAggregation
{
    private static final SetDigestStateSerializer SERIALIZER = new SetDigestStateSerializer();

    private BuildSetDigestAggregation() {}

    @InputFunction
    public static void input(SetDigestState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.getDigest() == null) {
            state.setDigest(new SetDigest());
        }
        state.getDigest().add(value);
    }

    @CombineFunction
    public static void combine(SetDigestState state, SetDigestState otherState)
    {
        if (state.getDigest() == null) {
            SetDigest copy = new SetDigest();
            copy.mergeWith(otherState.getDigest());
            state.setDigest(copy);
        }
        else {
            state.getDigest().mergeWith(otherState.getDigest());
        }
    }

    @OutputFunction(SetDigestType.NAME)
    public static void output(SetDigestState state, BlockBuilder out)
    {
        SERIALIZER.serialize(state, out);
    }
}
