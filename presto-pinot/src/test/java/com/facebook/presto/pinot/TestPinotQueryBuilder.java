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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.pinot.PinotQueryBuilder.getColumnPredicate;
import static com.facebook.presto.pinot.PinotQueryBuilder.getDiscretePredicate;
import static com.facebook.presto.pinot.PinotQueryBuilder.getPinotQuery;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestPinotQueryBuilder
{
    private static List<PinotColumnHandle> columnHandles = Arrays.asList(
            new PinotColumnHandle("varchar", VARCHAR, 0),
            new PinotColumnHandle("int", INTEGER, 1),
            new PinotColumnHandle("secondsSinceEpoch", BIGINT, 2),
            new PinotColumnHandle("boolean", BOOLEAN, 3),
            new PinotColumnHandle("double", DOUBLE, 4));

    @Test
    public void testGetPinotQuerySelectAll()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table  LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandles, "", "", "table", 10));
    }

    @Test
    public void testGetPinotQueryWithPredicate()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table WHERE ((int > 3)) AND ((secondsSinceEpoch > 10000)) LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandles, "(int > 3)", "(secondsSinceEpoch > 10000)", "table", 10));
    }

    @Test
    public void testSingleValueRanges()
    {
        Domain domain = com.facebook.presto.spi.predicate.Domain.multipleValues(BIGINT, new ArrayList<>(Arrays.asList(1L, 10L)));
        String expectedFilter = "int IN (1,10)";

        Assert.assertEquals(expectedFilter, getColumnPredicate(domain, columnHandles.get(1).getColumnName()));
    }

    @Test
    public void testRangeValues()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L))), false);

        String expectedFilter = "(1 < int AND int < 10)";
        Assert.assertEquals(expectedFilter, getColumnPredicate(domain, columnHandles.get(1).getColumnName()));
    }

    @Test
    public void testOneSideRanges()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.lessThanOrEqual(BIGINT, 10L)), false);

        String expectedFilter = "(int <= 10)";
        Assert.assertEquals(expectedFilter, getColumnPredicate(domain, columnHandles.get(1).getColumnName()));
    }

    @Test
    public void testMultipleRanges()
    {
        Domain domain = Domain.create(ValueSet.ofRanges(
                Range.equal(BIGINT, 20L),
                Range.greaterThan(BIGINT, 1L).intersect(Range.lessThan(BIGINT, 10L)),
                Range.greaterThan(BIGINT, 12L).intersect(Range.lessThan(BIGINT, 18L))), false);

        String expectedFilter = "(1 < int AND int < 10) OR (12 < int AND int < 18) OR int IN (20)";
        Assert.assertEquals(expectedFilter, getColumnPredicate(domain, columnHandles.get(1).getColumnName()));
    }

    @Test
    public void testGetDiscretePredicate()
    {
        Assert.assertEquals("", getDiscretePredicate(true, "int", new ArrayList<>()));
        Assert.assertEquals("int IN (1,2)", getDiscretePredicate(true, "int", new ArrayList<>(Arrays.asList("1", "2"))));
        Assert.assertEquals("int NOT IN (1,2)", getDiscretePredicate(false, "int", new ArrayList<>(Arrays.asList("1", "2"))));
        Assert.assertEquals("varchar NOT IN (\"cn\",\"us\")", getDiscretePredicate(false, "varchar", new ArrayList<>(Arrays.asList("\"cn\"", "\"us\""))));
    }

    @Test
    public void testEmptyDomain()
    {
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, new ArrayList<>());
        Domain domain = Domain.create(sortedRangeSet, false);
        Assert.assertEquals("", getColumnPredicate(domain, columnHandles.get(0).getColumnName()));
    }
}
