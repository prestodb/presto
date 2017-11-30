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

package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.operator.aggregation.SingleTypedHistogram;
import com.facebook.presto.operator.aggregation.TypedHistogram;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

public class SingleHistogramState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleHistogramState.class).instanceSize();
    private SingleTypedHistogram typedHistogram = null;

    public SingleHistogramState(Type keyType, int expectedEntriesCount)
    {
        typedHistogram = new SingleTypedHistogram(keyType, expectedEntriesCount);
    }

    @Override
    public TypedHistogram get()
    {
        return typedHistogram;
    }

    @Override
    public void set(TypedHistogram value)
    {
        if (value instanceof SingleTypedHistogram) {
            typedHistogram = (SingleTypedHistogram) value;
        }
        else {
            slowPathSet(value);
        }
    }

    @Override
    public void deserialize(Block block, Type type, int expectedSize)
    {
        typedHistogram = new SingleTypedHistogram(block, type, expectedSize);
    }

    @Override
    public void addMemoryUsage(long memory)
    {
    }

    @Override
    public long getEstimatedSize()
    {
        long estimatedSize = INSTANCE_SIZE;
        if (typedHistogram != null) {
            estimatedSize += typedHistogram.getEstimatedSize();
        }
        return estimatedSize;
    }

    // used in combine when the LHS state is not settable to he RHS
    private void slowPathSet(TypedHistogram value)
    {
        typedHistogram = new SingleTypedHistogram(value.getType(), value.getExpectedSize());
        typedHistogram.addAll(value);
    }
}
