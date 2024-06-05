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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree;

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class HistogramLevel
        implements QuantileTreeLevel
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HistogramLevel.class).instanceSize();
    private final float[] bins;

    public HistogramLevel(int length)
    {
        this.bins = new float[length];
    }

    public HistogramLevel(SliceInput s)
    {
        int size = s.readInt();
        this.bins = new float[size];

        for (int i = 0; i < size; i++) {
            bins[i] = s.readFloat();
        }
    }

    public QuantileTreeLevelFormat getFormat()
    {
        return QuantileTreeLevelFormat.HISTOGRAM;
    }

    public void merge(QuantileTreeLevel other)
    {
        checkArgument(getFormat() == other.getFormat(), "Cannot merge levels of different format");
        HistogramLevel otherHistogram = (HistogramLevel) other;
        checkArgument(bins.length == otherHistogram.bins.length, "Cannot merge histogramLevel with different size");
        for (int i = 0; i < bins.length; i++) {
            bins[i] += otherHistogram.bins[i];
        }
    }

    public void add(long item)
    {
        bins[itemIndex(item)] += 1;
    }

    public float query(long item)
    {
        return bins[itemIndex(item)];
    }

    public void addNoise(double rho, RandomizationStrategy randomizationStrategy)
    {
        checkArgument(rho > 0, "rho must be positive");

        // Under unbounded-neighbors semantics (add/remove), we have a squared L2 sensitivity of 1.
        // To achieve rho-zCDP, we add Gaussian noise with:
        // variance = 1 / (2 * rho), i.e., standard deviation = sqrt(0.5 / rho)
        for (int i = 0; i < bins.length; i++) {
            bins[i] += randomizationStrategy.nextGaussian() * Math.sqrt(0.5 / rho);
        }
    }

    private static int itemIndex(long item)
    {
        checkArgument(item <= Integer.MAX_VALUE, "Cannot insert items greater than Integer.MAX_VALUE");
        return (int) item;
    }

    public int size()
    {
        return bins.length;
    }

    public int estimatedSerializedSizeInBytes()
    {
        return SIZE_OF_INT + bins.length * SIZE_OF_FLOAT;
    }

    public Slice serialize()
    {
        int requiredBytes = estimatedSerializedSizeInBytes();

        SliceOutput s = new DynamicSliceOutput(requiredBytes);
        s.writeInt(bins.length);
        for (float bin : bins) {
            s.writeFloat(bin);
        }

        return s.slice();
    }

    public long estimatedSizeInBytes()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(bins);
    }
}
