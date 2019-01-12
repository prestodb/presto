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
package io.prestosql.operator;

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.Type;
import io.prestosql.util.MergeSortedPages.PageWithPosition;

import java.io.Closeable;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.util.MergeSortedPages.mergeSortedPages;

/**
 * This class performs merge of previously hash sorted pages streams.
 * <p>
 * Positions are compared using their hash value. It is possible
 * that two distinct values to have same hash value, thus returned
 * stream of Pages can have interleaved positions with same hash value.
 */
public class MergeHashSort
        implements Closeable
{
    private final AggregatedMemoryContext memoryContext;

    public MergeHashSort(AggregatedMemoryContext memoryContext)
    {
        this.memoryContext = memoryContext;
    }

    /**
     * Rows with same hash value are guaranteed to be in the same result page.
     */
    public WorkProcessor<Page> merge(List<Type> keyTypes, List<Type> allTypes, List<WorkProcessor<Page>> channels, DriverYieldSignal driverYieldSignal)
    {
        InterpretedHashGenerator hashGenerator = createHashGenerator(keyTypes);
        return mergeSortedPages(
                channels,
                createHashPageWithPositionComparator(hashGenerator),
                IntStream.range(0, allTypes.size()).boxed().collect(toImmutableList()),
                allTypes,
                keepSameHashValuesWithinSinglePage(hashGenerator),
                true,
                memoryContext,
                driverYieldSignal);
    }

    @Override
    public void close()
    {
        memoryContext.close();
    }

    private static BiPredicate<PageBuilder, PageWithPosition> keepSameHashValuesWithinSinglePage(InterpretedHashGenerator hashGenerator)
    {
        return (pageBuilder, pageWithPosition) -> {
            long hash = hashGenerator.hashPosition(pageWithPosition.getPosition(), pageWithPosition.getPage());
            return !pageBuilder.isEmpty()
                    && hashGenerator.hashPosition(pageBuilder.getPositionCount() - 1, pageBuilder::getBlockBuilder) != hash
                    && pageBuilder.isFull();
        };
    }

    private static PageWithPositionComparator createHashPageWithPositionComparator(HashGenerator hashGenerator)
    {
        return (Page leftPage, int leftPosition, Page rightPage, int rightPosition) -> {
            long leftHash = hashGenerator.hashPosition(leftPosition, leftPage);
            long rightHash = hashGenerator.hashPosition(rightPosition, rightPage);

            return Long.compare(leftHash, rightHash);
        };
    }

    private static InterpretedHashGenerator createHashGenerator(List<Type> keyTypes)
    {
        int[] hashChannels = new int[keyTypes.size()];
        for (int i = 0; i < keyTypes.size(); i++) {
            hashChannels[i] = i;
        }
        return new InterpretedHashGenerator(keyTypes, hashChannels);
    }
}
