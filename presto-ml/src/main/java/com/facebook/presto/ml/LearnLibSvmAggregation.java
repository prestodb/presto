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
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import libsvm.svm_parameter;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class LearnLibSvmAggregation
        implements AggregationFunction
{
    private final Type modelType;
    private final Type labelType;

    public LearnLibSvmAggregation(Type modelType, Type labelType)
    {
        this.modelType = modelType;
        this.labelType = labelType;
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
        throw new UnsupportedOperationException("LEARN must run on a single machine");
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeight, double confidence, int... argumentChannels)
    {
        checkArgument(!maskChannel.isPresent(), "masking is not supported");
        checkArgument(confidence == 1, "approximation is not supported");
        checkArgument(!sampleWeight.isPresent(), "sample weight is not supported");
        return new LearnAccumulator(argumentChannels[0], argumentChannels[1], argumentChannels[2], labelType == BIGINT, modelType == RegressorType.REGRESSOR);
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        throw new UnsupportedOperationException("LEARN must run on a single machine");
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeight, double confidence, int... argumentChannels)
    {
        throw new UnsupportedOperationException("LEARN doesn't support GROUP BY");
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
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
            BlockCursor cursor = page.getBlock(labelChannel).cursor();
            while (cursor.advanceNextPosition()) {
                if (labelIsLong) {
                    labels.add((double) cursor.getLong());
                }
                else {
                    labels.add(cursor.getDouble());
                }
            }

            cursor = page.getBlock(featuresChannel).cursor();
            while (cursor.advanceNextPosition()) {
                FeatureVector featureVector = ModelUtils.jsonToFeatures(cursor.getSlice());
                rowsSize += featureVector.getEstimatedSize();
                rows.add(featureVector);
            }

            if (params == null) {
                cursor = page.getBlock(paramsChannel).cursor();
                cursor.advanceNextPosition();
                params = LibSvmUtils.parseParameters(cursor.getSlice().toStringUtf8());
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
                model = new RegressorFeatureTransformer(new SvmRegressor(params), new FeatureVectorUnitNormalizer());
            }
            else {
                model = new ClassifierFeatureTransformer(new SvmClassifier(params), new FeatureVectorUnitNormalizer());
            }

            model.train(dataset);

            BlockBuilder builder = getFinalType().createBlockBuilder(new BlockBuilderStatus());
            builder.appendSlice(ModelUtils.serialize(model));

            return builder.build();
        }
    }
}
