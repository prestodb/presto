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
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.type.setdigest.SetDigestType.SET_DIGEST;

@AggregationFunction("merge_set_digest")
public final class MergeSetDigestAggregation
{
    private MergeSetDigestAggregation() {}

    @InputFunction
    public static void input(SetDigestState state, @SqlType(SetDigestType.NAME) Slice value)
    {
        SetDigest instance = SetDigest.newInstance(value);
        merge(state, instance);
    }

    @CombineFunction
    public static void combine(SetDigestState state, SetDigestState otherState)
    {
        merge(state, otherState.getDigest());
    }

    private static void merge(SetDigestState state, SetDigest instance)
    {
        if (state.getDigest() == null) {
            state.setDigest(instance);
        }
        else {
            state.getDigest().mergeWith(instance);
        }
    }

    @OutputFunction(SetDigestType.NAME)
    public static void output(SetDigestState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            SET_DIGEST.writeSlice(out, state.getDigest().serialize());
        }
    }
}
