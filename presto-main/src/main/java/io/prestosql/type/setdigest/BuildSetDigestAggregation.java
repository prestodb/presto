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

package io.prestosql.type.setdigest;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

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
