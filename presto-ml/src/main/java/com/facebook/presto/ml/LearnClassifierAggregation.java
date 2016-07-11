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
package com.facebook.presto.ml;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;

@AggregationFunction(value = "learn_classifier", decomposable = false)
public final class LearnClassifierAggregation
{
    private LearnClassifierAggregation() {}

    @InputFunction
    public static void input(
            LearnState state,
            @SqlType(BIGINT) long label,
            @SqlType("map(bigint,double)") Block features)
    {
        input(state, (double) label, features);
    }

    @InputFunction
    public static void input(
            LearnState state,
            @SqlType(DOUBLE) double label,
            @SqlType("map(bigint,double)") Block features)
    {
        LearnLibSvmClassifierAggregation.input(state, label, features, Slices.utf8Slice(""));
    }

    @CombineFunction
    public static void combine(LearnState state, LearnState otherState)
    {
        throw new UnsupportedOperationException("LEARN must run on a single machine");
    }

    @OutputFunction("Classifier<bigint>")
    public static void output(LearnState state, BlockBuilder out)
    {
        LearnLibSvmClassifierAggregation.output(state, out);
    }
}
