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
package io.trino.parquet.writer.repdef;

import com.google.common.collect.AbstractIterator;
import io.trino.parquet.writer.repdef.RepLevelIterable.RepValueIterator;
import io.trino.parquet.writer.repdef.RepLevelIterable.RepetitionLevel;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RepLevelIterables
{
    private RepLevelIterables() {}

    public static RepLevelIterable of(Block block)
    {
        return new BlockRepLevel(block);
    }

    public static RepLevelIterable of(ColumnarArray columnarArray, int maxRepLevel)
    {
        return new ArrayRepLevel(columnarArray, maxRepLevel);
    }

    public static RepLevelIterable of(ColumnarMap columnarMap, int maxRepLevel)
    {
        return new MapRepLevel(columnarMap, maxRepLevel);
    }

    public static Iterator<Integer> getIterator(List<RepLevelIterable> iterables)
    {
        return new NestedRepLevelIterator(iterables);
    }

    static class BlockRepLevel
            implements RepLevelIterable
    {
        private final Block block;

        BlockRepLevel(Block block)
        {
            this.block = requireNonNull(block, "block is null");
        }

        @Override
        public RepValueIterator getIterator()
        {
            return new RepValueIterator()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected RepetitionLevel computeNext()
                {
                    position++;
                    if (position == block.getPositionCount()) {
                        return endOfData();
                    }
                    if (block.isNull(position)) {
                        return nullValue(getBase());
                    }
                    return nonNullValue(getBase());
                }
            };
        }
    }

    static class ArrayRepLevel
            implements RepLevelIterable
    {
        private final ColumnarArray columnarArray;
        private final int maxRepValue;

        ArrayRepLevel(ColumnarArray columnarArray, int maxRepValue)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxRepValue = maxRepValue;
        }

        @Override
        public RepValueIterator getIterator()
        {
            return new RepValueIterator()
            {
                private int position = -1;
                private FixedValueIterator iterator;

                @Override
                boolean end()
                {
                    return iterator == null || !iterator.hasNext();
                }

                @Override
                protected RepetitionLevel computeNext()
                {
                    if (iterator != null && iterator.hasNext()) {
                        return iterator.next();
                    }
                    position++;
                    if (position == columnarArray.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarArray.isNull(position)) {
                        return nullValue(getBase());
                    }
                    int arrayLength = columnarArray.getLength(position);
                    if (arrayLength == 0) {
                        return nullValue(getBase());
                    }
                    iterator = new FixedValueIterator(arrayLength, getBase(), maxRepValue);
                    return iterator.next();
                }
            };
        }
    }

    static class MapRepLevel
            implements RepLevelIterable
    {
        private final ColumnarMap columnarArray;
        private final int maxRepValue;

        MapRepLevel(ColumnarMap columnarArray, int maxRepValue)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxRepValue = maxRepValue;
        }

        @Override
        public RepValueIterator getIterator()
        {
            return new RepValueIterator()
            {
                private int position = -1;
                private FixedValueIterator iterator;

                @Override
                boolean end()
                {
                    return iterator == null || !iterator.hasNext();
                }

                @Override
                protected RepetitionLevel computeNext()
                {
                    if (iterator != null && iterator.hasNext()) {
                        return iterator.next();
                    }
                    position++;
                    if (position == columnarArray.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarArray.isNull(position)) {
                        return nullValue(getBase());
                    }
                    int arrayLength = columnarArray.getEntryCount(position);
                    if (arrayLength == 0) {
                        return nullValue(getBase());
                    }
                    iterator = new FixedValueIterator(arrayLength, getBase(), maxRepValue);
                    return iterator.next();
                }
            };
        }
    }

    static class NestedRepLevelIterator
            extends AbstractIterator<Integer>
    {
        private final List<RepValueIterator> repValueIteratorList;
        private int iteratorIndex;

        NestedRepLevelIterator(List<RepLevelIterable> repValueIteratorList)
        {
            this.repValueIteratorList = repValueIteratorList.stream().map(RepLevelIterable::getIterator).collect(toImmutableList());
        }

        @Override
        protected Integer computeNext()
        {
            RepValueIterator current = repValueIteratorList.get(iteratorIndex);
            while (iteratorIndex > 0 && current.end()) {
                current = repValueIteratorList.get(--iteratorIndex);
            }

            while (current.hasNext()) {
                RepetitionLevel currentRepValue = current.next();
                if (currentRepValue.isNull() || iteratorIndex == repValueIteratorList.size() - 1) {
                    return currentRepValue.value();
                }
                int lastValue = currentRepValue.value();
                current = repValueIteratorList.get(iteratorIndex + 1);
                current.setBase(lastValue);
                iteratorIndex++;
            }
            checkState(repValueIteratorList.stream().noneMatch(AbstractIterator::hasNext));
            return endOfData();
        }
    }

    static class FixedValueIterator
            extends AbstractIterator<RepetitionLevel>
    {
        private final int length;
        private final int parentValue;
        private final int currentValue;
        private int position = -1;

        FixedValueIterator(int length, int parentValue, int currentValue)
        {
            this.length = length;
            this.parentValue = parentValue;
            this.currentValue = currentValue;
        }

        @Override
        protected RepetitionLevel computeNext()
        {
            position++;
            if (position < length) {
                if (position == 0) {
                    return nonNullValue(parentValue);
                }
                return nonNullValue(currentValue);
            }
            return endOfData();
        }
    }

    private static RepetitionLevel nullValue(int value)
    {
        return new RepetitionLevel(value, true);
    }

    private static RepetitionLevel nonNullValue(int value)
    {
        return new RepetitionLevel(value, false);
    }
}
