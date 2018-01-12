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

import com.facebook.presto.array.BooleanBigArray;
import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class GroupedAreaUnderRocCurve
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedAreaUnderRocCurve.class).instanceSize();
    private static final int NULL = -1;

    // one entry per group
    // each entry is the index of the first elements of the group in the labels/scores/nextLinks arrays
    private final LongBigArray headIndices;

    // one entry per double/boolean pair
    private final BooleanBigArray labels;
    private final DoubleBigArray scores;

    // the value in nextLinks contains the index of the next value in the chain
    // a value NULL (-1) indicates it is the last value for the group
    private final IntBigArray nextLinks;

    // the index of the next free element in the labels/scores/nextLinks arrays
    // this is needed to be able to know where to continue adding elements when after the arrays are resized
    private int nextFreeIndex;

    private long currentGroupId = -1;

    public GroupedAreaUnderRocCurve()
    {
        this.headIndices = new LongBigArray(NULL);
        this.labels = new BooleanBigArray();
        this.scores = new DoubleBigArray();
        this.nextLinks = new IntBigArray(NULL);
        this.nextFreeIndex = 0;
    }

    /**
     * @param serialized a block of the from [label1, score1, label2, score2, ...]
     */
    public GroupedAreaUnderRocCurve(long groupId, Block serialized)
    {
        this();

        this.currentGroupId = groupId;

        // deserialize the serialized block
        requireNonNull(serialized, "block is null");
        for (int i = 0; i < serialized.getPositionCount(); i++) {
            Block entryBlock = serialized.getObject(i, Block.class);
            add(entryBlock, entryBlock, 0, 1);
        }
    }

    public void serialize(BlockBuilder out)
    {
        if (isCurrentGroupEmpty()) {
            out.appendNull();
        }
        else {
            List<Boolean> labelList = new ArrayList<>();
            List<Double> scoreList = new ArrayList<>();

            // retrieve values in each group
            int currentIndex = (int) headIndices.get(currentGroupId);
            while (currentIndex != NULL) {
                labelList.add(labels.get(currentIndex));
                scoreList.add(scores.get(currentIndex));
                currentIndex = nextLinks.get(currentIndex);
            }

            // convert lists to primitive arrays
            boolean[] labels = new boolean[labelList.size()];
            for (int i = 0; i < labels.length; i++) {
                labels[i] = labelList.get(i);
            }
            double[] scores = scoreList.stream().mapToDouble(Double::doubleValue).toArray();

            double auc = computeAUC(labels, scores);
            if (Double.isFinite(auc)) {
                DOUBLE.writeDouble(out, auc);
            }
            else {
                out.appendNull();
            }
        }
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE
                + labels.sizeOf()
                + scores.sizeOf()
                + nextLinks.sizeOf()
                + headIndices.sizeOf();
    }

    public GroupedAreaUnderRocCurve setGroupId(long groupId)
    {
        this.currentGroupId = groupId;
        return this;
    }

    public void add(Block labelsBlock, Block scoresBlock, int labelPosition, int scorePosition)
    {
        checkState(currentGroupId != -1, "setGroupId() has not been called yet");

        ensureCapacity(currentGroupId + 1);

        labels.set(nextFreeIndex, BOOLEAN.getBoolean(labelsBlock, labelPosition));
        scores.set(nextFreeIndex, DOUBLE.getDouble(scoresBlock, scorePosition));
        nextLinks.set(nextFreeIndex, (int) headIndices.get(currentGroupId));

        headIndices.set(currentGroupId, nextFreeIndex);
        nextFreeIndex++;
    }

    public void ensureCapacity(long numberOfGroups)
    {
        headIndices.ensureCapacity(numberOfGroups);

        int numberOfValues = nextFreeIndex + 1;
        labels.ensureCapacity(numberOfValues);
        scores.ensureCapacity(numberOfValues);
        nextLinks.ensureCapacity(numberOfValues);
    }

    public void addAll(GroupedAreaUnderRocCurve other)
    {
        other.readAll(this);
    }

    private void readAll(GroupedAreaUnderRocCurve to)
    {
        int currentIndex = (int) headIndices.get(currentGroupId);
        while (currentIndex != NULL) {
            BlockBuilder labelBlockBuilder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 1);
            BOOLEAN.writeBoolean(labelBlockBuilder, labels.get(currentIndex));

            BlockBuilder scoreBlockBuilder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 1);
            DOUBLE.writeDouble(scoreBlockBuilder, scores.get(currentIndex));

            to.add(labelBlockBuilder.build(), scoreBlockBuilder.build(), 0, 0);

            currentIndex = nextLinks.get(currentIndex);
        }
    }

    public boolean isCurrentGroupEmpty()
    {
        return headIndices.get(currentGroupId) == NULL;
    }

    /**
     * Calculate Area Under the ROC Curve using trapezoidal rule
     * See Algorithm 4 in a paper "ROC Graphs: Notes and Practical Considerations
     * for Data Mining Researchers" for detail:
     * http://www.hpl.hp.com/techreports/2003/HPL-2003-4.pdf
     */
    private static double computeAUC(boolean[] labels, double[] scores)
    {
        // labels sorted descending by scores
        int[] indices = new int[labels.length];
        for (int i = 0; i < labels.length; i++) {
            indices[i] = i;
        }
        IntArrays.quickSort(indices, new AbstractIntComparator()
        {
            @Override
            public int compare(int i, int j)
            {
                return Double.compare(scores[j], scores[i]);
            }
        });

        int truePositive = 0;
        int falsePositive = 0;

        int truePositivePrev = 0;
        int falsePositivePrev = 0;

        double area = 0.0;

        double scorePrev = Double.POSITIVE_INFINITY;

        for (int i : indices) {
            if (Double.compare(scores[i], scorePrev) != 0) {
                area += trapezoidArea(falsePositive, falsePositivePrev, truePositive, truePositivePrev);

                scorePrev = scores[i];
                falsePositivePrev = falsePositive;
                truePositivePrev = truePositive;
            }
            if (labels[i]) {
                truePositive++;
            }
            else {
                falsePositive++;
            }
        }

        // AUC for single class outcome is not defined
        if (truePositive == 0 || falsePositive == 0) {
            return Double.POSITIVE_INFINITY;
        }

        // finalize by adding a trapezoid area based on the final tp/fp counts
        area += trapezoidArea(falsePositive, falsePositivePrev, truePositive, truePositivePrev);

        return area / (truePositive * falsePositive); // scaled value in the 0-1 range
    }

    /**
     * Calculate trapezoidal area under (x1, y1)-(x2, y2)
     */
    private static double trapezoidArea(double x1, double x2, double y1, double y2)
    {
        double base = Math.abs(x1 - x2);
        double averageHeight = (y1 + y2) / 2;
        return base * averageHeight;
    }
}
