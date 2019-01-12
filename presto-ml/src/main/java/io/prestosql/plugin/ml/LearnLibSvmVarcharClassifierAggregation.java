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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.AggregationState;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;

import static io.prestosql.plugin.ml.type.ClassifierType.VARCHAR_CLASSIFIER;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;

@AggregationFunction(value = "learn_libsvm_classifier", decomposable = false)
public final class LearnLibSvmVarcharClassifierAggregation
{
    private LearnLibSvmVarcharClassifierAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState LearnState state,
            @SqlType(VARCHAR) Slice label,
            @SqlType("map(bigint,double)") Block features,
            @SqlType(VARCHAR) Slice parameters)
    {
        state.getLabels().add((double) state.enumerateLabel(label.toStringUtf8()));
        FeatureVector featureVector = ModelUtils.toFeatures(features);
        state.addMemoryUsage(featureVector.getEstimatedSize());
        state.getFeatureVectors().add(featureVector);
        state.setParameters(parameters);
    }

    @CombineFunction
    public static void combine(@AggregationState LearnState state, @AggregationState LearnState otherState)
    {
        throw new UnsupportedOperationException("LEARN must run on a single machine");
    }

    @OutputFunction("Classifier<varchar>")
    public static void output(@AggregationState LearnState state, BlockBuilder out)
    {
        Dataset dataset = new Dataset(state.getLabels(), state.getFeatureVectors(), state.getLabelEnumeration().inverse());
        Model model = new StringClassifierAdapter(new ClassifierFeatureTransformer(new SvmClassifier(LibSvmUtils.parseParameters(state.getParameters().toStringUtf8())), new FeatureUnitNormalizer()));
        model.train(dataset);
        VARCHAR_CLASSIFIER.writeSlice(out, ModelUtils.serialize(model));
    }
}
