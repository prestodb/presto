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

package io.prestosql.operator.aggregation.histogram;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

public class SingleHistogramState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleHistogramState.class).instanceSize();

    private SingleTypedHistogram typedHistogram;

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
}
