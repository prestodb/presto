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

import com.facebook.presto.ml.type.RegressorType;
import com.facebook.presto.spi.Page;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import libsvm.svm_parameter;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.ml.type.ClassifierType.CLASSIFIER;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class LearnLibSvmAggregation
        implements InternalAggregationFunction
{
    private final Type modelType;
    private final Type labelType;

    public LearnLibSvmAggregation(Type modelType, Type labelType)
    {
        this.modelType = modelType;
        this.labelType = labelType;
    }

    @Override
    public String name()
    {
        return modelType == CLASSIFIER ? "learn_libsvm_classifier" : "learn_libsvm_regressor";
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.of(labelType, VARCHAR, VARCHAR);
    }

    @Override
    public Type getFinalType()
    {
        return modelType;
    }

    @Override
    public Type getIntermediateType()
    {
        return UNKNOWN;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public boolean isApproximate()
    {
        return false;
    }

    @Override
    public AccumulatorFactory bind(List<Integer> inputChannels, Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence)
    {
        checkArgument(!maskChannel.isPresent(), "masking is not supported");
        checkArgument(confidence == 1, "approximation is not supported");
        checkArgument(!sampleWeightChannel.isPresent(), "sample weight is not supported");
        return new LearnLibSvmAccumulatorFactory(inputChannels, labelType == BIGINT, modelType == RegressorType.REGRESSOR);
    }

    public static class LearnLibSvmAccumulatorFactory
            implements AccumulatorFactory
    {
        private final List<Integer> inputChannels;
        private final boolean labelIsLong;
        private final boolean regression;

        public LearnLibSvmAccumulatorFactory(List<Integer> inputChannels, boolean labelIsLong, boolean regression)
        {
            this.inputChannels = ImmutableList.copyOf(checkNotNull(inputChannels, "inputChannels is null"));
            this.labelIsLong = labelIsLong;
            this.regression = regression;
        }

        @Override
        public List<Integer> getInputChannels()
        {
            return inputChannels;
        }

        @Override
        public Accumulator createAccumulator()
        {
            return new LearnAccumulator(inputChannels.get(0), inputChannels.get(1), inputChannels.get(2), labelIsLong, regression);
        }

        @Override
        public Accumulator createIntermediateAccumulator()
        {
            throw new UnsupportedOperationException("LEARN must run on a single machine");
        }

        @Override
        public GroupedAccumulator createGroupedAccumulator()
        {
            throw new UnsupportedOperationException("LEARN doesn't support GROUP BY");
        }

        @Override
        public GroupedAccumulator createGroupedIntermediateAccumulator()
        {
            throw new UnsupportedOperationException("LEARN doesn't support GROUP BY");
        }

        public static class LearnAccumulator
                implements Accumulator
        {
            private final int labelChannel;
            private final int featuresChannel;
            private final int paramsChannel;
            private final boolean labelIsLong;
            private final boolean regression;
            private final List<Double> labels = new ArrayList<>();
            private final List<FeatureVector> rows = new ArrayList<>();
            private long rowsSize;
            private svm_parameter params;

            public LearnAccumulator(int labelChannel, int featuresChannel, int paramsChannel, boolean labelIsLong, boolean regression)
            {
                this.labelChannel = labelChannel;
                this.featuresChannel = featuresChannel;
                this.paramsChannel = paramsChannel;
                this.labelIsLong = labelIsLong;
                this.regression = regression;
            }

            @Override
            public long getEstimatedSize()
            {
                return SIZE_OF_DOUBLE * (long) labels.size() + rowsSize;
            }

            @Override
            public Type getFinalType()
            {
                return VARCHAR;
            }

            @Override
            public Type getIntermediateType()
            {
                throw new UnsupportedOperationException("LEARN must run on a single machine");
            }

            @Override
            public void addInput(Page page)
            {
                Block block = page.getBlock(labelChannel);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    if (labelIsLong) {
                        labels.add((double) BIGINT.getLong(block, position));
                    }
                    else {
                        labels.add(DOUBLE.getDouble(block, position));
                    }
                }

                block = page.getBlock(featuresChannel);
                for (int position = 0; position < block.getPositionCount(); position++) {
                    FeatureVector featureVector = ModelUtils.jsonToFeatures(VARCHAR.getSlice(block, position));
                    rowsSize += featureVector.getEstimatedSize();
                    rows.add(featureVector);
                }

                if (params == null) {
                    block = page.getBlock(paramsChannel);
                    params = LibSvmUtils.parseParameters(VARCHAR.getSlice(block, 0).toStringUtf8());
                }
            }

            @Override
            public void addIntermediate(Block block)
            {
                throw new UnsupportedOperationException("LEARN must run on a single machine");
            }

            @Override
            public Block evaluateIntermediate()
            {
                throw new UnsupportedOperationException("LEARN must run on a single machine");
            }

            @Override
            public Block evaluateFinal()
            {
                Dataset dataset = new Dataset(labels, rows);

                Model model;

                if (regression) {
                    model = new RegressorFeatureTransformer(new SvmRegressor(params), new FeatureUnitNormalizer());
                }
                else {
                    model = new ClassifierFeatureTransformer(new SvmClassifier(params), new FeatureUnitNormalizer());
                }

                model.train(dataset);

                BlockBuilder builder = getFinalType().createBlockBuilder(new BlockBuilderStatus());
                getFinalType().writeSlice(builder, ModelUtils.serialize(model));

                return builder.build();
            }
        }
    }
}
