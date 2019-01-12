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
package io.prestosql.plugin.ml;

import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;

import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;

@AggregationFunction(value = "learn_classifier", decomposable = false)
public final class LearnClassifierAggregation
{
    private LearnClassifierAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState LearnState state,
            @SqlType(BIGINT) long label,
            @SqlType("map(bigint,double)") Block features)
    {
        input(state, (double) label, features);
    }

    @InputFunction
    public static void input(
            @AggregationState LearnState state,
            @SqlType(DOUBLE) double label,
            @SqlType("map(bigint,double)") Block features)
    {
        LearnLibSvmClassifierAggregation.input(state, label, features, Slices.utf8Slice(""));
    }

    @CombineFunction
    public static void combine(@AggregationState LearnState state, @AggregationState LearnState otherState)
    {
        throw new UnsupportedOperationException("LEARN must run on a single machine");
    }

    @OutputFunction("Classifier<bigint>")
    public static void output(@AggregationState LearnState state, BlockBuilder out)
    {
        LearnLibSvmClassifierAggregation.output(state, out);
    }
}
