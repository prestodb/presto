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
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hudi.common.HudiColumnStatsConverter.toDomain;
import static com.facebook.presto.hudi.common.HudiStatisticsEvaluator.mayMatch;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHudiStatisticsEvaluator
{
    @Test
    public void testAllAndNone()
    {
        assertTrue(mayMatch(TupleDomain.all(), Map.of()));
        assertFalse(mayMatch(TupleDomain.none(), Map.of()));
        assertFalse(mayMatch(TupleDomain.none(), Map.of("x", Domain.all(BIGINT))));
    }

    @Test
    public void testPrunesOnlyDisjointFiles()
    {
        TupleDomain<String> predicate = predicate(Domain.singleValue(BIGINT, 10L));
        assertFalse(mayMatch(predicate, Map.of("x", toDomain(BIGINT, 3, 0, 1L, 9L))));
        assertTrue(mayMatch(predicate, Map.of("x", toDomain(BIGINT, 3, 0, 1L, 10L))));
        assertTrue(mayMatch(predicate, Map.of("x", toDomain(BIGINT, 3, 0, 10L, 20L))));
        assertFalse(mayMatch(predicate, Map.of("x", toDomain(BIGINT, 3, 0, 11L, 20L))));
    }

    @Test
    public void testMissingAndMismatchedStatistics()
    {
        TupleDomain<String> predicate = predicate(Domain.singleValue(BIGINT, 10L));
        assertTrue(mayMatch(predicate, Map.of()));
        assertTrue(mayMatch(predicate, Map.of("unrelated", Domain.singleValue(BIGINT, 1L))));
        assertTrue(mayMatch(predicate, Map.of("x", Domain.all(BIGINT))));
        assertTrue(mayMatch(predicate, Map.of("x", Domain.singleValue(INTEGER, 1L))));
    }

    @Test
    public void testOneKnownConjunctCanRuleOutFile()
    {
        TupleDomain<String> predicate = TupleDomain.withColumnDomains(Map.of(
                "unknown", Domain.singleValue(BIGINT, 10L),
                "x", Domain.singleValue(BIGINT, 20L)));
        assertFalse(mayMatch(predicate, Map.of("x", toDomain(BIGINT, 3, 0, 1L, 10L))));
        assertTrue(mayMatch(predicate, Map.of("x", toDomain(BIGINT, 3, 0, 1L, 30L))));
    }

    @Test
    public void testDisjointRangesAndIn()
    {
        Domain ranges = Domain.create(ValueSet.ofRanges(
                Range.lessThan(BIGINT, 5L),
                Range.greaterThan(BIGINT, 15L)), false);
        Domain middleFile = toDomain(BIGINT, 3, 0, 5L, 15L);
        assertFalse(mayMatch(predicate(ranges), Map.of("x", middleFile)));
        assertTrue(mayMatch(predicate(ranges), Map.of("x", toDomain(BIGINT, 3, 0, 4L, 15L))));

        Domain discrete = Domain.multipleValues(BIGINT, List.of(4L, 16L));
        assertFalse(mayMatch(predicate(discrete), Map.of("x", middleFile)));
        assertTrue(mayMatch(predicate(discrete), Map.of("x", toDomain(BIGINT, 3, 0, 5L, 16L))));
    }

    @Test
    public void testNullPredicates()
    {
        Domain nonNulls = toDomain(BIGINT, 3, 0, 1L, 2L);
        Domain allNulls = toDomain(BIGINT, 3, 3, null, null);
        Domain mixed = toDomain(BIGINT, 3, 1, 1L, 2L);
        assertFalse(mayMatch(predicate(Domain.onlyNull(BIGINT)), Map.of("x", nonNulls)));
        assertTrue(mayMatch(predicate(Domain.onlyNull(BIGINT)), Map.of("x", allNulls)));
        assertTrue(mayMatch(predicate(Domain.onlyNull(BIGINT)), Map.of("x", mixed)));
        assertFalse(mayMatch(predicate(Domain.notNull(BIGINT)), Map.of("x", allNulls)));
        assertTrue(mayMatch(predicate(Domain.notNull(BIGINT)), Map.of("x", nonNulls)));

        Domain equalityOrNull = Domain.create(ValueSet.of(BIGINT, 10L), true);
        assertFalse(mayMatch(predicate(equalityOrNull), Map.of("x", nonNulls)));
        assertTrue(mayMatch(predicate(equalityOrNull), Map.of("x", mixed)));
    }

    @Test
    public void testNotEqualDoesNotInvertMayMatch()
    {
        Domain notTen = Domain.singleValue(BIGINT, 10L).complement();
        assertFalse(mayMatch(predicate(notTen), Map.of("x", toDomain(BIGINT, 3, 0, 10L, 10L))));
        assertTrue(mayMatch(predicate(notTen), Map.of("x", toDomain(BIGINT, 3, 0, 9L, 11L))));
    }

    @Test
    public void testMatchingRowsAreNeverPruned()
    {
        Random random = new Random(0);
        for (int iteration = 0; iteration < 500; iteration++) {
            List<Long> values = new ArrayList<>();
            long nullCount = 0;
            Long minimum = null;
            Long maximum = null;
            int size = random.nextInt(20);
            for (int i = 0; i < size; i++) {
                if (random.nextInt(4) == 0) {
                    values.add(null);
                    nullCount++;
                }
                else {
                    long value = random.nextInt(101) - 50;
                    values.add(value);
                    minimum = minimum == null ? value : Math.min(minimum, value);
                    maximum = maximum == null ? value : Math.max(maximum, value);
                }
            }
            Domain fileDomain = toDomain(BIGINT, values.size(), nullCount, minimum, maximum);
            long boundary = random.nextInt(101) - 50;
            List<Domain> domains = List.of(
                    Domain.all(BIGINT),
                    Domain.none(BIGINT),
                    Domain.onlyNull(BIGINT),
                    Domain.notNull(BIGINT),
                    Domain.singleValue(BIGINT, boundary),
                    Domain.singleValue(BIGINT, boundary).complement(),
                    Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, boundary)), false),
                    Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, boundary)), false),
                    Domain.create(ValueSet.of(BIGINT, boundary), true),
                    Domain.multipleValues(BIGINT, List.of(boundary, boundary + 10)));
            for (Domain domain : domains) {
                if (values.stream().anyMatch(domain::includesNullableValue)) {
                    assertTrue(mayMatch(predicate(domain), Map.of("x", fileDomain)), "Pruned a file containing a matching row");
                }
            }
        }
    }

    private static TupleDomain<String> predicate(Domain domain)
    {
        return TupleDomain.withColumnDomains(Map.of("x", domain));
    }
}
