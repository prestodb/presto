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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.TupleExpression;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTupleExpressionOrcPredicate
{
    private static final Type SHORT_DECIMAL = createDecimalType(5, 2);
    private static final Type LONG_DECIMAL = createDecimalType(20, 10);
    private static final Type CHAR = createCharType(10);

    @Test
    public void testTupleExpressionOrcPredicate()
            throws Exception
    {
        Domain domain1 = Domain.multipleValues(BIGINT, Arrays.asList(1L, 2L, 3L));
        Domain domain2 = Domain.singleValue(VARCHAR, Slices.utf8Slice("test"));
        TupleExpression expression1 = new DomainExpression<String>("columnA", domain1);
        TupleExpression expression2 = new DomainExpression<String>("columnB", domain2);
        TupleExpression finalExpression = new OrExpression<String>(expression1, expression2);
        Map<String, TupleExpressionOrcPredicate.ColumnReference<String>> map = new HashMap<>();
        map.put("columnA", new TupleExpressionOrcPredicate.ColumnReference<String>("columnA", 1, BIGINT));
        map.put("columnB", new TupleExpressionOrcPredicate.ColumnReference<String>("columnB", 2, VARCHAR));
        Map<Integer, ColumnStatistics> columnStatisticsMap = new HashMap<>();
        columnStatisticsMap.put(0, new ColumnStatistics(100L, null, null, null, null, null,
                null, null));
        columnStatisticsMap.put(1, new ColumnStatistics(100L, null,
                new IntegerStatistics(0L, 1L), null, null, null,
                null, null));
        columnStatisticsMap.put(2, new ColumnStatistics(100L, null, null, null,
                new StringStatistics(Slices.utf8Slice("test"), Slices.utf8Slice("test")), null,
                null, null));
        TupleExpressionOrcPredicate predicate = new TupleExpressionOrcPredicate(finalExpression, map, false);
        assertTrue(predicate.matches(100, columnStatisticsMap));
        columnStatisticsMap.put(0, new ColumnStatistics(100L, null, null, null, null, null,
                null, null));
        columnStatisticsMap.put(1, new ColumnStatistics(100L, null,
                new IntegerStatistics(110L, 1000L), null, null, null,
                null, null));
        columnStatisticsMap.put(2, new ColumnStatistics(100L, null, null, null,
                new StringStatistics(Slices.utf8Slice("test"), Slices.utf8Slice("test")), null,
                null, null));
        predicate = new TupleExpressionOrcPredicate(finalExpression, map, false);
        assertTrue(predicate.matches(100, columnStatisticsMap));
        columnStatisticsMap.put(0, new ColumnStatistics(100L, null, null, null, null, null,
                null, null));
        columnStatisticsMap.put(1, new ColumnStatistics(100L, null,
                new IntegerStatistics(10000L, 1000000L), null, null, null,
                null, null));
        columnStatisticsMap.put(2, new ColumnStatistics(100L, null, null, null,
                new StringStatistics(Slices.utf8Slice("test2"), Slices.utf8Slice("test2")), null,
                null, null));
        predicate = new TupleExpressionOrcPredicate(finalExpression, map, false);
        assertFalse(predicate.matches(100, columnStatisticsMap));
    }
}
