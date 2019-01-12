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
package io.prestosql.operator.aggregation;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;

public class TypedHeap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TypedHeap.class).instanceSize();

    private static final int COMPACT_THRESHOLD_BYTES = 32768;
    private static final int COMPACT_THRESHOLD_RATIO = 3; // when 2/3 of elements in heapBlockBuilder is unreferenced, do compact

    private final BlockComparator comparator;
    private final Type type;
    private final int capacity;

    private int positionCount;
    private final int[] heapIndex;
    private BlockBuilder heapBlockBuilder;

    public TypedHeap(BlockComparator comparator, Type type, int capacity)
    {
        this.comparator = comparator;
        this.type = type;
        this.capacity = capacity;
        this.heapIndex = new int[capacity];
        this.heapBlockBuilder = type.createBlockBuilder(null, capacity);
    }

    public int getCapacity()
    {
        return capacity;
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + heapBlockBuilder.getRetainedSizeInBytes() + sizeOf(heapIndex);
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public void writeAll(BlockBuilder resultBlockBuilder)
    {
        for (int i = 0; i < positionCount; i++) {
            type.appendTo(heapBlockBuilder, heapIndex[i], resultBlockBuilder);
        }
    }

    public void popAll(BlockBuilder resultBlockBuilder)
    {
        while (positionCount > 0) {
            pop(resultBlockBuilder);
        }
    }

    public void pop(BlockBuilder resultBlockBuilder)
    {
        type.appendTo(heapBlockBuilder, heapIndex[0], resultBlockBuilder);
        remove();
    }

    private void remove()
    {
        positionCount--;
        heapIndex[0] = heapIndex[positionCount];
        siftDown();
    }

    public void add(Block block, int position)
    {
        checkArgument(!block.isNull(position));
        if (positionCount == capacity) {
            if (comparator.compareTo(heapBlockBuilder, heapIndex[0], block, position) >= 0) {
                return; // and new element is not larger than heap top: do not add
            }
            heapIndex[0] = heapBlockBuilder.getPositionCount();
            type.appendTo(block, position, heapBlockBuilder);
            siftDown();
        }
        else {
            heapIndex[positionCount] = heapBlockBuilder.getPositionCount();
            positionCount++;
            type.appendTo(block, position, heapBlockBuilder);
            siftUp();
        }
        compactIfNecessary();
    }

    public void addAll(TypedHeap other)
    {
        for (int i = 0; i < other.positionCount; i++) {
            add(other.heapBlockBuilder, other.heapIndex[i]);
        }
    }

    public void addAll(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            add(block, i);
        }
    }

    private void siftDown()
    {
        int position = 0;
        while (true) {
            int leftPosition = position * 2 + 1;
            if (leftPosition >= positionCount) {
                break;
            }
            int rightPosition = leftPosition + 1;
            int smallerChildPosition;
            if (rightPosition >= positionCount) {
                smallerChildPosition = leftPosition;
            }
            else {
                smallerChildPosition = comparator.compareTo(heapBlockBuilder, heapIndex[leftPosition], heapBlockBuilder, heapIndex[rightPosition]) >= 0 ? rightPosition : leftPosition;
            }
            if (comparator.compareTo(heapBlockBuilder, heapIndex[smallerChildPosition], heapBlockBuilder, heapIndex[position]) >= 0) {
                break; // child is larger or equal
            }
            int swapTemp = heapIndex[position];
            heapIndex[position] = heapIndex[smallerChildPosition];
            heapIndex[smallerChildPosition] = swapTemp;
            position = smallerChildPosition;
        }
    }

    private void siftUp()
    {
        int position = positionCount - 1;
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (comparator.compareTo(heapBlockBuilder, heapIndex[position], heapBlockBuilder, heapIndex[parentPosition]) >= 0) {
                break; // child is larger or equal
            }
            int swapTemp = heapIndex[position];
            heapIndex[position] = heapIndex[parentPosition];
            heapIndex[parentPosition] = swapTemp;
            position = parentPosition;
        }
    }

    private void compactIfNecessary()
    {
        // Byte size check is needed. Otherwise, if size * 3 is small, BlockBuilder can be reallocate too often.
        // Position count is needed. Otherwise, for large elements, heap will be compacted every time.
        // Size instead of retained size is needed because default allocation size can be huge for some block builders. And the first check will become useless in such case.
        if (heapBlockBuilder.getSizeInBytes() < COMPACT_THRESHOLD_BYTES || heapBlockBuilder.getPositionCount() / positionCount < COMPACT_THRESHOLD_RATIO) {
            return;
        }
        BlockBuilder newHeapBlockBuilder = type.createBlockBuilder(null, heapBlockBuilder.getPositionCount());
        for (int i = 0; i < positionCount; i++) {
            type.appendTo(heapBlockBuilder, heapIndex[i], newHeapBlockBuilder);
            heapIndex[i] = i;
        }
        heapBlockBuilder = newHeapBlockBuilder;
    }
}
