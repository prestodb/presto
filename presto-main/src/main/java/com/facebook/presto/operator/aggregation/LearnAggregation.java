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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.ml.Dataset;
import com.facebook.presto.ml.FeatureVector;
import com.facebook.presto.ml.FeatureVectorUnitNormalizer;
import com.facebook.presto.ml.Model;
import com.facebook.presto.ml.ModelUtils;
import com.facebook.presto.ml.SvmClassifier;
import com.facebook.presto.ml.SvmRegressor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.DoubleMath;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class LearnAggregation
        implements AggregationFunction
{
    private final TupleInfo.Type labelType;

    public LearnAggregation(TupleInfo.Type labelType)
    {
        this.labelType = labelType;
    }

    @Override
    public List<TupleInfo.Type> getParameterTypes()
    {
        return ImmutableList.of(labelType, TupleInfo.Type.VARIABLE_BINARY);
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
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
        return new LearnAccumulator(argumentChannels[0], argumentChannels[1], labelType == TupleInfo.Type.FIXED_INT_64);
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
        public TupleInfo getFinalTupleInfo()
        {
            return TupleInfo.SINGLE_VARBINARY;
        }

        @Override
        public TupleInfo getIntermediateTupleInfo()
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
                model = new FeatureVectorUnitNormalizer(new SvmRegressor());
            }
            else {
                model = new FeatureVectorUnitNormalizer(new SvmClassifier());
            }

            model.train(dataset);

            BlockBuilder builder = new BlockBuilder(TupleInfo.SINGLE_VARBINARY);
            builder.append(ModelUtils.serialize(model));

            return builder.build();
        }
    }
}
