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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.ml.type.ClassifierType.VARCHAR_CLASSIFIER;

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
