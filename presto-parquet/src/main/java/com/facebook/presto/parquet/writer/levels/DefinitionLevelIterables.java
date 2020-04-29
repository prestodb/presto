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
package com.facebook.presto.parquet.writer.levels;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterable.DefinitionLevelIterator;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class DefinitionLevelIterables
{
    private DefinitionLevelIterables() {}

    public static DefinitionLevelIterable of(Block block, int maxDefinitionLevel)
    {
        return new PrimitiveDefinitionLevelIterable(block, maxDefinitionLevel);
    }

    public static DefinitionLevelIterable of(ColumnarRow columnarRow, int maxDefinitionLevel)
    {
        return new ColumnRowDefinitionLevelIterable(columnarRow, maxDefinitionLevel);
    }

    public static DefinitionLevelIterable of(ColumnarArray columnarArray, int maxDefinitionLevel)
    {
        return new ColumnArrayDefinitionLevelIterable(columnarArray, maxDefinitionLevel);
    }

    public static DefinitionLevelIterable of(ColumnarMap columnarMap, int maxDefinitionLevel)
    {
        return new ColumnMapDefinitionLevelIterable(columnarMap, maxDefinitionLevel);
    }

    public static Iterator<Integer> getIterator(List<DefinitionLevelIterable> iterables)
    {
        return new NestedDefinitionLevelIterator(iterables);
    }

    static class PrimitiveDefinitionLevelIterable
            implements DefinitionLevelIterable
    {
        private final Block block;
        private final int maxDefinitionLevel;

        PrimitiveDefinitionLevelIterable(Block block, int maxDefinitionLevel)
        {
            this.block = requireNonNull(block, "block is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelIterator getIterator()
        {
            return new DefinitionLevelIterator()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected OptionalInt computeNext()
                {
                    position++;
                    if (position == block.getPositionCount()) {
                        return endOfData();
                    }
                    if (block.isNull(position)) {
                        return OptionalInt.of(maxDefinitionLevel - 1);
                    }
                    return OptionalInt.of(maxDefinitionLevel);
                }
            };
        }
    }

    static class ColumnRowDefinitionLevelIterable
            implements DefinitionLevelIterable
    {
        private final ColumnarRow columnarRow;
        private final int maxDefinitionLevel;

        ColumnRowDefinitionLevelIterable(ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            this.columnarRow = requireNonNull(columnarRow, "columnarRow is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelIterator getIterator()
        {
            return new DefinitionLevelIterator()
            {
                private int position = -1;

                @Override
                boolean end()
                {
                    return true;
                }

                @Override
                protected OptionalInt computeNext()
                {
                    position++;
                    if (position == columnarRow.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarRow.isNull(position)) {
                        return OptionalInt.of(maxDefinitionLevel - 1);
                    }
                    return OptionalInt.empty();
                }
            };
        }
    }

    static class ColumnMapDefinitionLevelIterable
            implements DefinitionLevelIterable
    {
        private final ColumnarMap columnarMap;
        private final int maxDefinitionLevel;

        ColumnMapDefinitionLevelIterable(ColumnarMap columnarMap, int maxDefinitionLevel)
        {
            this.columnarMap = requireNonNull(columnarMap, "columnarMap is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelIterator getIterator()
        {
            return new DefinitionLevelIterator()
            {
                private int position = -1;
                private Iterator<OptionalInt> iterator;

                @Override
                boolean end()
                {
                    return iterator == null || !iterator.hasNext();
                }

                @Override
                protected OptionalInt computeNext()
                {
                    if (iterator != null && iterator.hasNext()) {
                        return iterator.next();
                    }
                    position++;
                    if (position == columnarMap.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarMap.isNull(position)) {
                        return OptionalInt.of(maxDefinitionLevel - 2);
                    }
                    int arrayLength = columnarMap.getEntryCount(position);
                    if (arrayLength == 0) {
                        return OptionalInt.of(maxDefinitionLevel - 1);
                    }
                    iterator = nCopies(arrayLength, OptionalInt.empty()).iterator();
                    return iterator.next();
                }
            };
        }
    }

    static class ColumnArrayDefinitionLevelIterable
            implements DefinitionLevelIterable
    {
        private final ColumnarArray columnarArray;
        private final int maxDefinitionLevel;

        ColumnArrayDefinitionLevelIterable(ColumnarArray columnarArray, int maxDefinitionLevel)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefinitionLevelIterator getIterator()
        {
            return new DefinitionLevelIterator()
            {
                private int position = -1;
                private Iterator<OptionalInt> iterator;

                @Override
                boolean end()
                {
                    return iterator == null || !iterator.hasNext();
                }

                @Override
                protected OptionalInt computeNext()
                {
                    if (iterator != null && iterator.hasNext()) {
                        return iterator.next();
                    }
                    position++;
                    if (position == columnarArray.getPositionCount()) {
                        return endOfData();
                    }
                    if (columnarArray.isNull(position)) {
                        return OptionalInt.of(maxDefinitionLevel - 2);
                    }
                    int arrayLength = columnarArray.getLength(position);
                    if (arrayLength == 0) {
                        return OptionalInt.of(maxDefinitionLevel - 1);
                    }
                    iterator = nCopies(arrayLength, OptionalInt.empty()).iterator();
                    return iterator.next();
                }
            };
        }
    }

    static class NestedDefinitionLevelIterator
            extends AbstractIterator<Integer>
    {
        private final List<DefinitionLevelIterator> iterators;
        private int iteratorIndex;

        NestedDefinitionLevelIterator(List<DefinitionLevelIterable> iterables)
        {
            this.iterators = iterables.stream().map(DefinitionLevelIterable::getIterator).collect(toImmutableList());
        }

        @Override
        protected Integer computeNext()
        {
            DefinitionLevelIterator current = iterators.get(iteratorIndex);
            while (iteratorIndex > 0 && current.end()) {
                iteratorIndex--;
                current = iterators.get(iteratorIndex);
            }

            while (current.hasNext()) {
                OptionalInt next = current.next();
                if (next.isPresent()) {
                    return next.getAsInt();
                }
                iteratorIndex++;
                current = iterators.get(iteratorIndex);
            }
            checkState(iterators.stream().noneMatch(AbstractIterator::hasNext));
            return endOfData();
        }
    }
}
