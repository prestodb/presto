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
import com.google.common.collect.ImmutableSet;
import com.google.common.math.DoubleMath;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class LearnAggregation
        implements AggregationFunction
{
    private final Type modelType;
    private final Type labelType;

    public LearnAggregation(Type modelType, Type labelType)
    {
        this.modelType = modelType;
        this.labelType = labelType;
    }

    @Override
    public List<Type> getParameterTypes()
    {
        return ImmutableList.of(labelType, VARCHAR);
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
        return new LearnAccumulator(argumentChannels[0], argumentChannels[1], labelType == BIGINT);
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
        private final boolean labelIsLong;
        //TODO: these could get very big, so they should be reported like the estimated memory for GroupedAccumulator
        private final List<Double> labels = new ArrayList<>();
        private final List<FeatureVector> rows = new ArrayList<>();

        public LearnAccumulator(int labelChannel, int featuresChannel, boolean labelIsLong)
        {
            this.labelChannel = labelChannel;
            this.featuresChannel = featuresChannel;
            this.labelIsLong = labelIsLong;
        }

        @Override
        public long getEstimatedSize()
        {
            long size = SIZE_OF_DOUBLE * (long) labels.size();
            for (FeatureVector vector : rows) {
                size += vector.getEstimatedSize();
            }
            return size;
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
                rows.add(ModelUtils.jsonToFeatures(cursor.getSlice()));
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

            // Heuristic to decide if this is a classification or a regression problem
            boolean regression = ImmutableSet.copyOf(labels).size() > 100;
            for (double label : labels) {
                if (!DoubleMath.isMathematicalInteger(label)) {
                    regression = true;
                    break;
                }
            }

            Model model;

            if (regression) {
                model = new RegressorFeatureTransformer(new SvmRegressor(), new FeatureVectorUnitNormalizer());
            }
            else {
                model = new ClassifierFeatureTransformer(new SvmClassifier(), new FeatureVectorUnitNormalizer());
            }

            model.train(dataset);

            BlockBuilder builder = getFinalType().createBlockBuilder(new BlockBuilderStatus());
            builder.appendSlice(ModelUtils.serialize(model));

            return builder.build();
        }
    }
}
