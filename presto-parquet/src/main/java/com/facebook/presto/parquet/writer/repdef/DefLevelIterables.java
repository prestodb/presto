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
package com.facebook.presto.parquet.writer.repdef;

import com.facebook.presto.parquet.writer.repdef.DefLevelIterable.DefLevelIterator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class DefLevelIterables
{
    private DefLevelIterables() {}

    public static DefLevelIterable of(Block block, int maxDefinitionLevel)
    {
        return new PrimitiveDefLevelIterable(block, maxDefinitionLevel);
    }

    public static DefLevelIterable of(ColumnarRow columnarRow, int maxDefinitionLevel)
    {
        return new ColumnRowDefLevelIterable(columnarRow, maxDefinitionLevel);
    }

    public static DefLevelIterable of(ColumnarArray columnarArray, int maxDefinitionLevel)
    {
        return new ColumnArrayDefLevelIterable(columnarArray, maxDefinitionLevel);
    }

    public static DefLevelIterable of(ColumnarMap columnarMap, int maxDefinitionLevel)
    {
        return new ColumnMapDefLevelIterable(columnarMap, maxDefinitionLevel);
    }

    public static Iterator<Integer> getIterator(List<DefLevelIterable> iterables)
    {
        return new NestedDefLevelIterator(iterables);
    }

    static class PrimitiveDefLevelIterable
            implements DefLevelIterable
    {
        private final Block block;
        private final int maxDefinitionLevel;

        PrimitiveDefLevelIterable(Block block, int maxDefinitionLevel)
        {
            this.block = requireNonNull(block, "block is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefLevelIterator getIterator()
        {
            return new DefLevelIterator()
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

    static class ColumnRowDefLevelIterable
            implements DefLevelIterable
    {
        private final ColumnarRow columnarRow;
        private final int maxDefinitionLevel;

        ColumnRowDefLevelIterable(ColumnarRow columnarRow, int maxDefinitionLevel)
        {
            this.columnarRow = requireNonNull(columnarRow, "columnarRow is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefLevelIterator getIterator()
        {
            return new DefLevelIterator()
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

    static class ColumnMapDefLevelIterable
            implements DefLevelIterable
    {
        private final ColumnarMap columnarMap;
        private final int maxDefinitionLevel;

        ColumnMapDefLevelIterable(ColumnarMap columnarMap, int maxDefinitionLevel)
        {
            this.columnarMap = requireNonNull(columnarMap, "columnarMap is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefLevelIterator getIterator()
        {
            return new DefLevelIterator()
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

    static class ColumnArrayDefLevelIterable
            implements DefLevelIterable
    {
        private final ColumnarArray columnarArray;
        private final int maxDefinitionLevel;

        ColumnArrayDefLevelIterable(ColumnarArray columnarArray, int maxDefinitionLevel)
        {
            this.columnarArray = requireNonNull(columnarArray, "columnarArray is null");
            this.maxDefinitionLevel = maxDefinitionLevel;
        }

        @Override
        public DefLevelIterator getIterator()
        {
            return new DefLevelIterator()
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

    static class NestedDefLevelIterator
            extends AbstractIterator<Integer>
    {
        private final List<DefLevelIterator> iterators;
        private int iteratorIndex;

        NestedDefLevelIterator(List<DefLevelIterable> iterables)
        {
            this.iterators = iterables.stream().map(DefLevelIterable::getIterator).collect(toImmutableList());
        }

        @Override
        protected Integer computeNext()
        {
            DefLevelIterator current = iterators.get(iteratorIndex);
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
