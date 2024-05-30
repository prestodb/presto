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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;

public class SketchedLevel
        implements QuantileTreeLevel
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SketchedLevel.class).instanceSize();

    private final CountSketch sketch;

    public SketchedLevel(int depth, int width)
    {
        sketch = new CountSketch(depth, width);
    }

    public SketchedLevel(SliceInput s)
    {
        sketch = new CountSketch(s);
    }

    public QuantileTreeLevelFormat getFormat()
    {
        return QuantileTreeLevelFormat.SKETCHED;
    }

    public void merge(QuantileTreeLevel other)
    {
        checkArgument(getFormat() == other.getFormat(), "Cannot merge levels of different format");
        CountSketch otherSketch = ((SketchedLevel) other).sketch;
        sketch.merge(otherSketch);
    }

    public void add(long item)
    {
        sketch.add(item);
    }

    public void addNoise(double rho, RandomizationStrategy randomizationStrategy)
    {
        sketch.addNoise(rho, 1, randomizationStrategy);
    }

    public float query(long item)
    {
        return sketch.estimateCount(item);
    }

    @Override
    public int size()
    {
        return sketch.getDepth() * sketch.getWidth();
    }

    public Slice serialize()
    {
        return sketch.serialize();
    }

    public int estimatedSerializedSizeInBytes()
    {
        return sketch.estimatedSerializedSizeInBytes();
    }

    public long estimatedSizeInBytes()
    {
        return INSTANCE_SIZE + sketch.estimatedSizeInBytes();
    }
}
