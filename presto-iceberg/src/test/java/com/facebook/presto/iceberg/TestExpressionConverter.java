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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DateType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExpressionConverter
{
    // 5000 is above the ~3000-partition threshold where the stack overflow was observed in production.
    private static final int LARGE_PARTITION_COUNT = 5_000;

    private static final IcebergColumnHandle CREATED_DATE_HANDLE = primitiveIcebergColumnHandle(
            1, "created_date", DateType.DATE, Optional.empty());

    private static final Types.StructType DATE_SCHEMA = Types.StructType.of(
            Types.NestedField.optional(1, "created_date", Types.DateType.get()));

    private static final Schema DATE_ICEBERG_SCHEMA = new Schema(
            Types.NestedField.optional(1, "created_date", Types.DateType.get()));

    // Regression test for the primary bug: a 5000-point domain must not overflow ExpressionVisitors.
    @Test
    public void testLargeMultiValueDomainProducesInExpression()
    {
        Domain domain = Domain.multipleValues(DateType.DATE, buildDateValues(LARGE_PARTITION_COUNT));
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);

        // Should not overflow — visiting a tree of depth O(log(N/200)) is safe.
        visitExpression(expression);
        assertTrue(evalOnDate(expression, 19000L), "first value should match");
        assertTrue(evalOnDate(expression, 19000L + LARGE_PARTITION_COUNT - 1), "last value should match");
        assertFalse(evalOnDate(expression, 18999L), "value before range should not match");
        assertFalse(evalOnDate(expression, 19000L + LARGE_PARTITION_COUNT), "value after range should not match");
    }

    // Single equality value takes the equal() branch, not in(); verify the short-circuit.
    @Test
    public void testSingleEqualityValueUsesEqualPredicate()
    {
        Domain domain = Domain.singleValue(DateType.DATE, 19000L);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 should match equal(19000)");
        assertFalse(evalOnDate(expression, 19001L), "19001 should not match equal(19000)");
    }

    // Small domains (100 values, within IN_PREDICATE_LIMIT) convert to a single in() after the fix.
    @Test
    public void testSmallDomainConvertsCorrectly()
    {
        Domain domain = Domain.multipleValues(DateType.DATE, buildDateValues(100));
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "first value should match");
        assertTrue(evalOnDate(expression, 19099L), "last value should match");
        assertFalse(evalOnDate(expression, 18999L), "value before range should not match");
        assertFalse(evalOnDate(expression, 19100L), "value after range should not match");
    }

    // Chunk boundary: at exactly 200 values a single in() is used; at 201 values it is split
    // into chunks and both must still prune a file whose bounds exclude all predicate values.
    @Test
    public void testChunkBoundaryPreservesPruning()
    {
        // File with created_date values in [15000, 15099]: below all predicate values (19000+).
        ByteBuffer lower = Conversions.toByteBuffer(Types.DateType.get(), 15000);
        ByteBuffer upper = Conversions.toByteBuffer(Types.DateType.get(), 15099);
        Metrics fileMetrics = new Metrics(
                100L,
                null,
                ImmutableMap.of(1, 100L),
                ImmutableMap.of(1, 0L),
                null,
                ImmutableMap.of(1, lower),
                ImmutableMap.of(1, upper));
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("test.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100)
                .withMetrics(fileMetrics)
                .build();

        // 200 values — single in() within IN_PREDICATE_LIMIT: file must be pruned.
        Domain domain200 = Domain.multipleValues(DateType.DATE, buildDateValues(200));
        Expression expr200 = ExpressionConverter.toIcebergExpression(
                TupleDomain.withColumnDomains(ImmutableMap.of(CREATED_DATE_HANDLE, domain200)));
        assertFalse(new InclusiveMetricsEvaluator(DATE_ICEBERG_SCHEMA, expr200).eval(file),
                "200-value domain should prune a file with no matching values");

        // 201 values — split into two chunks; each chunk must independently prune the file.
        Domain domain201 = Domain.multipleValues(DateType.DATE, buildDateValues(201));
        Expression expr201 = ExpressionConverter.toIcebergExpression(
                TupleDomain.withColumnDomains(ImmutableMap.of(CREATED_DATE_HANDLE, domain201)));
        assertFalse(new InclusiveMetricsEvaluator(DATE_ICEBERG_SCHEMA, expr201).eval(file),
                "201-value domain should still prune a file with no matching values (chunked in()s)");
    }

    // Null-allowed domain must include an isNull() predicate OR'd with the value predicate.
    @Test
    public void testNullAllowedDomainIncludesIsNull()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.equal(DateType.DATE, 19000L))),
                true);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 should match");
        assertFalse(evalOnDate(expression, 19001L), "19001 should not match");
        assertTrue(evalOnNull(expression), "null should match null-allowed domain");
    }

    // Regression test for wrong-results bug: a null-allowed half-open range (x > a AND x < b)
    // used to produce and(or(isNull, gt(a)), lt(b)) which caused InclusiveMetricsEvaluator to
    // prune all-null files (valueCount=0 makes lt() return ROWS_CANNOT_MATCH, so and(_, CANNOT)
    // = CANNOT_MATCH). Null rows that satisfied IS NULL were silently dropped.
    // New code produces or(isNull, and(gt(a), lt(b))): or(HAS_NULLS, CANNOT) = ROWS_MIGHT_MATCH.
    @Test
    public void testNullAllowedHalfOpenRangePreservesNullFilesInMetrics()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.range(DateType.DATE, 19010L, false, 19020L, false))),
                true);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));
        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);

        assertTrue(evalOnNull(expression), "null rows must match a null-allowed domain");

        // An all-null file must NOT be pruned: the IS NULL branch can match.
        Metrics allNullMetrics = new Metrics(
                10L,
                null,
                ImmutableMap.of(1, 0L),   // no non-null values
                ImmutableMap.of(1, 10L),  // all 10 rows are null
                null,
                null,
                null);
        DataFile allNullFile = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("test-null.parquet")
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(100)
                .withMetrics(allNullMetrics)
                .build();
        assertTrue(new InclusiveMetricsEvaluator(DATE_ICEBERG_SCHEMA, expression).eval(allNullFile),
                "an all-null file must not be pruned when IS NULL is part of the predicate");
    }

    // Upper-bounded-only range (date < 19050) must match values below the bound and exclude the bound itself.
    @Test
    public void testUpperBoundedOnlyRangeConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.lessThan(DateType.DATE, 19050L))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 is below 19050, should match");
        assertTrue(evalOnDate(expression, 19049L), "19049 is below 19050, should match");
        assertFalse(evalOnDate(expression, 19050L), "19050 is the exclusive upper bound, should not match");
        assertFalse(evalOnDate(expression, 19100L), "19100 is above 19050, should not match");
    }

    // BETWEEN range (lo <= date <= hi, lo != hi) exercises the cachedLow/cachedHigh paths in pass 2.
    @Test
    public void testBetweenRangeConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.range(DateType.DATE, 19000L, true, 19100L, true))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 is the inclusive lower bound, should match");
        assertTrue(evalOnDate(expression, 19050L), "19050 is inside the range, should match");
        assertTrue(evalOnDate(expression, 19100L), "19100 is the inclusive upper bound, should match");
        assertFalse(evalOnDate(expression, 18999L), "18999 is below the range, should not match");
        assertFalse(evalOnDate(expression, 19101L), "19101 is above the range, should not match");
    }

    // Domain mixing point equalities with an open range: each branch is evaluated independently.
    @Test
    public void testMixedEqualityAndRangeDomainConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.equal(DateType.DATE, 19000L),
                        Range.equal(DateType.DATE, 19001L),
                        Range.greaterThan(DateType.DATE, 19100L))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19000L), "19000 is an equality value, should match");
        assertTrue(evalOnDate(expression, 19001L), "19001 is an equality value, should match");
        assertTrue(evalOnDate(expression, 19101L), "19101 is above 19100, should match");
        assertFalse(evalOnDate(expression, 19002L), "19002 is not in any range, should not match");
        assertFalse(evalOnDate(expression, 19099L), "19099 is below the open range, should not match");
        assertFalse(evalOnDate(expression, 19100L), "19100 is the exclusive lower bound, should not match");
    }

    // Equality value above a bounded open range must remain an independent disjunct.
    // This verifies that the two-pass rewrite preserves mixed-range semantics.
    @Test
    public void testEqualityValueAboveBoundedRangeConvertsCorrectly()
    {
        Domain domain = Domain.create(
                SortedRangeSet.copyOf(DateType.DATE, ImmutableList.of(
                        Range.range(DateType.DATE, 19010L, false, 19020L, false),
                        Range.equal(DateType.DATE, 19050L))),
                false);
        TupleDomain<IcebergColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(CREATED_DATE_HANDLE, domain));

        Expression expression = ExpressionConverter.toIcebergExpression(tupleDomain);
        assertTrue(evalOnDate(expression, 19011L), "19011 is inside (19010, 19020), should match");
        assertTrue(evalOnDate(expression, 19019L), "19019 is inside (19010, 19020), should match");
        assertTrue(evalOnDate(expression, 19050L), "19050 is the equality value, should match");
        assertFalse(evalOnDate(expression, 19010L), "19010 is the exclusive lower bound, should not match");
        assertFalse(evalOnDate(expression, 19020L), "19020 is the exclusive upper bound, should not match");
        assertFalse(evalOnDate(expression, 19021L), "19021 is between the range and equality, should not match");
        assertFalse(evalOnDate(expression, 19049L), "19049 is between the range and equality, should not match");
    }

    // --- helpers ---

    private static List<Long> buildDateValues(int count)
    {
        return LongStream.range(19000, 19000 + count)
                .boxed()
                .collect(ImmutableList.toImmutableList());
    }

    private static boolean evalOnDate(Expression expression, long prestoDateValue)
    {
        int icebergDateValue = toIntExact(prestoDateValue);
        Evaluator evaluator = new Evaluator(DATE_SCHEMA, expression);
        return evaluator.eval(new StructLike()
        {
            @Override
            public int size()
            {
                return 1;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T> T get(int pos, Class<T> javaClass)
            {
                return (T) (Integer) icebergDateValue;
            }

            @Override
            public <T> void set(int pos, T value) {}
        });
    }

    private static boolean evalOnNull(Expression expression)
    {
        Evaluator evaluator = new Evaluator(DATE_SCHEMA, expression);
        return evaluator.eval(new StructLike()
        {
            @Override
            public int size()
            {
                return 1;
            }

            @Override
            public <T> T get(int pos, Class<T> javaClass)
            {
                return null;
            }

            @Override
            public <T> void set(int pos, T value) {}
        });
    }

    private static void visitExpression(Expression expression)
    {
        ExpressionVisitors.visit(expression, new ExpressionVisitor<String>()
        {
            @Override
            public String alwaysTrue()
            {
                return "TRUE";
            }

            @Override
            public String alwaysFalse()
            {
                return "FALSE";
            }

            @Override
            public String not(String result)
            {
                return "NOT(" + result + ")";
            }

            @Override
            public String and(String left, String right)
            {
                return "AND(" + left + "," + right + ")";
            }

            @Override
            public String or(String left, String right)
            {
                return "OR(" + left + "," + right + ")";
            }

            @Override
            public <T> String predicate(BoundPredicate<T> pred)
            {
                return pred.toString();
            }

            @Override
            public <T> String predicate(UnboundPredicate<T> pred)
            {
                return pred.toString();
            }
        });
    }
}
