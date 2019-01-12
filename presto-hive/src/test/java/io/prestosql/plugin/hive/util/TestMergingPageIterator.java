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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMergingPageIterator
{
    @Test
    public void testMerging()
    {
        List<Type> types = ImmutableList.of(INTEGER, INTEGER);
        List<Integer> sortIndexes = ImmutableList.of(1);
        List<SortOrder> sortOrders = ImmutableList.of(SortOrder.ASC_NULLS_FIRST);

        List<List<Page>> pageLists = new ArrayList<>();
        PageBuilder pageBuilder = new PageBuilder(types);

        for (int i = 0; i < 10; i++) {
            Iterator<Integer> values = IntStream.range(0, 1000)
                    .map(ignored -> ThreadLocalRandom.current().nextInt(100_000))
                    .mapToObj(n -> ((n % 100) == 0) ? null : n)
                    .sorted(nullsFirst(naturalOrder()))
                    .iterator();
            List<Page> pages = new ArrayList<>();
            for (int j = 0; j < 10; j++) {
                for (int k = 0; k < 100; k++) {
                    Integer n = values.next();
                    pageBuilder.declarePosition();
                    if (n == null) {
                        pageBuilder.getBlockBuilder(0).appendNull();
                        pageBuilder.getBlockBuilder(1).appendNull();
                    }
                    else {
                        INTEGER.writeLong(pageBuilder.getBlockBuilder(0), n);
                        INTEGER.writeLong(pageBuilder.getBlockBuilder(1), n * 22L);
                    }
                }
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
            pageLists.add(pages);
            assertFalse(values.hasNext());
        }

        List<Iterator<Page>> pages = pageLists.stream()
                .map(List::iterator)
                .collect(toList());
        Iterator<Page> iterator = new MergingPageIterator(pages, types, sortIndexes, sortOrders);

        List<Long> values = new ArrayList<>();
        while (iterator.hasNext()) {
            Page page = iterator.next();
            for (int i = 0; i < page.getPositionCount(); i++) {
                if (page.getBlock(0).isNull(i)) {
                    assertTrue(page.getBlock(1).isNull(i));
                    values.add(null);
                }
                else {
                    long x = INTEGER.getLong(page.getBlock(0), i);
                    long y = INTEGER.getLong(page.getBlock(1), i);
                    assertEquals(y, x * 22);
                    values.add(x);
                }
            }
        }

        assertThat(values).isSortedAccordingTo(nullsFirst(naturalOrder()));
    }
}
