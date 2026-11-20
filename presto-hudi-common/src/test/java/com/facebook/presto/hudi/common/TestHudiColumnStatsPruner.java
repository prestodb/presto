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
package com.facebook.presto.hudi.common;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import org.apache.hudi.common.util.collection.Pair;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hudi.common.HudiColumnStatsPruner.MAX_BATCH_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;

public class TestHudiColumnStatsPruner
{
    @Test
    public void testPartitionAndFileIdentity()
            throws IOException
    {
        Pair<String, String> first = Pair.of("p=1", "same.parquet");
        Pair<String, String> second = Pair.of("p=2", "same.parquet");
        Pair<String, String> missing = Pair.of("p=1", "missing.parquet");
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            assertEquals(files, List.of(first, second, missing));
            assertEquals(columns, Map.of("x", BIGINT));
            return Map.of(
                    first, Map.of("x", Domain.singleValue(BIGINT, 1L)),
                    second, Map.of("x", Domain.singleValue(BIGINT, 10L)));
        });
        assertEquals(pruner.retainFiles(List.of(first, second, missing)), Set.of(second, missing));
    }

    @Test
    public void testNoLookupForUnusablePredicates()
            throws IOException
    {
        List<Pair<String, String>> files = List.of(Pair.of("", "file.parquet"));
        HudiColumnStatsReader reader = (requestedFiles, columns) -> {
            throw new AssertionError("Unexpected statistics lookup");
        };
        assertEquals(new HudiColumnStatsPruner(TupleDomain.all(), reader).retainFiles(files), Set.copyOf(files));
        assertEquals(new HudiColumnStatsPruner(TupleDomain.none(), reader).retainFiles(files), Set.of());
        assertEquals(new HudiColumnStatsPruner(predicate(), reader).retainFiles(List.of()), Set.of());
        HudiColumnStatsPruner unsupported = new HudiColumnStatsPruner(
                TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(DOUBLE, 10.0))), reader);
        assertFalse(unsupported.isUseful());
        assertEquals(unsupported.retainFiles(files), Set.copyOf(files));
    }

    @Test
    public void testUnsupportedConjunctDoesNotDisableSupportedColumn()
            throws IOException
    {
        Pair<String, String> file = Pair.of("", "file.parquet");
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(
                TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(BIGINT, 10L), "y", Domain.singleValue(DOUBLE, 1.0))),
                (files, columns) -> {
                    assertEquals(columns, Map.of("x", BIGINT));
                    return Map.of(file, Map.of("x", Domain.singleValue(BIGINT, 1L)));
                });
        assertEquals(pruner.retainFiles(List.of(file)), Set.of());
    }

    @Test
    public void testLookupFailureIsNotAnEmptyResult()
    {
        IOException failure = new IOException("metadata unavailable");
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(predicate(), (files, columns) -> {
            throw failure;
        });
        assertEquals(expectThrows(IOException.class, () -> pruner.retainFiles(List.of(Pair.of("", "file.parquet")))), failure);
    }

    @Test
    public void testLookupBounds()
            throws IOException
    {
        HudiColumnStatsReader reader = (files, columns) -> {
            throw new AssertionError("Unbounded lookup");
        };
        List<Pair<String, String>> files = IntStream.range(0, MAX_BATCH_SIZE + 1)
                .mapToObj(i -> Pair.of("", i + ".parquet"))
                .toList();
        expectThrows(IllegalArgumentException.class, () -> new HudiColumnStatsPruner(predicate(), reader).retainFiles(files));

        Map<String, Domain> wide = new HashMap<>();
        IntStream.range(0, 100).forEach(i -> wide.put("column_" + i, Domain.singleValue(BIGINT, 10L)));
        HudiColumnStatsPruner pruner = new HudiColumnStatsPruner(TupleDomain.withColumnDomains(wide), reader);
        assertFalse(pruner.isUseful());
        assertEquals(pruner.retainFiles(files.subList(0, 1)), Set.of(files.get(0)));
    }

    private static TupleDomain<String> predicate()
    {
        return TupleDomain.withColumnDomains(Map.of("x", Domain.singleValue(BIGINT, 10L)));
    }
}
