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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Created by praveen-3307 on 14/4/17.
 */
public class TestTupleExpression
{
    private static final ColumnHandle A = new TestingColumnHandle("a");
    private static final ColumnHandle B = new TestingColumnHandle("b");
    private static final ColumnHandle C = new TestingColumnHandle("c");
    private static final ColumnHandle D = new TestingColumnHandle("d");
    private static final ColumnHandle E = new TestingColumnHandle("e");
    private static final ColumnHandle F = new TestingColumnHandle("f");

    @Test
    public void testNone()
            throws Exception
    {
        TupleExpression expression1 = new DomainExpression<ColumnHandle>(A, Domain.none(INTEGER));
        TupleExpression expression2 = new DomainExpression<ColumnHandle>(B, Domain.singleValue(BIGINT, 1L));
        TupleExpression expression3 = new DomainExpression<ColumnHandle>(C, Domain.all(VARCHAR));
        TupleExpression expression4 = new DomainExpression<ColumnHandle>(D, Domain.none(TINYINT));
        TupleExpression expression5 = new DomainExpression<ColumnHandle>(E, Domain.all(DOUBLE));
        assertTrue(new NoneExpression<ColumnHandle>().isNone());
        assertTrue(expression1.isNone());
        assertTrue(new AndExpression<ColumnHandle>(expression2, new NoneExpression<ColumnHandle>()).isNone());
        assertFalse(new OrExpression<ColumnHandle>(expression2, new NoneExpression<ColumnHandle>()).isNone());
        assertTrue(new NotExpression<ColumnHandle>(new AllExpression()).isNone());
        assertTrue(new NotExpression<ColumnHandle>(expression5).isNone());
        assertTrue(new AndExpression<ColumnHandle>(expression3, expression4).isNone());
        assertFalse(new OrExpression<ColumnHandle>(expression3, expression4).isNone());
    }

    @Test
    public void testAll()
            throws Exception
    {
        TupleExpression expression1 = new DomainExpression<ColumnHandle>(A, Domain.all(INTEGER));
        TupleExpression expression2 = new DomainExpression<ColumnHandle>(B, Domain.singleValue(BIGINT, 1L));
        TupleExpression expression3 = new DomainExpression<ColumnHandle>(C, Domain.none(VARCHAR));
        TupleExpression expression4 = new DomainExpression<ColumnHandle>(D, Domain.all(TINYINT));
        TupleExpression expression5 = new DomainExpression<ColumnHandle>(E, Domain.none(DOUBLE));
        assertTrue(new AllExpression<ColumnHandle>().isAll());
        assertTrue(expression1.isAll());
        assertFalse(new AndExpression<ColumnHandle>(expression2, new AllExpression<ColumnHandle>()).isAll());
        assertTrue(new OrExpression<ColumnHandle>(expression2, new AllExpression<ColumnHandle>()).isAll());
        assertTrue(new NotExpression<ColumnHandle>(new NoneExpression()).isAll());
        assertTrue(new NotExpression<ColumnHandle>(expression5).isAll());
        assertFalse(new AndExpression<ColumnHandle>(expression3, expression4).isAll());
        assertTrue(new OrExpression<ColumnHandle>(expression3, expression4).isAll());
    }

    @Test
    public void testTransform()
            throws Exception
    {
        TupleExpression expression1 = new DomainExpression<Integer>(1, Domain.singleValue(BIGINT, 1L));
        TupleExpression expression2 = new DomainExpression<Integer>(2, Domain.singleValue(BIGINT, 2L));
        TupleExpression expression3 = new DomainExpression<Integer>(3, Domain.singleValue(BIGINT, 3L));

        TupleExpression combinedExpression = new NotExpression<Integer>(
                new OrExpression<Integer>(expression3, new AndExpression<Integer>(expression1, expression2)));

        TupleExpression<String> transformed = combinedExpression.transform(Object::toString);

        TupleExpression expression1Transformed = new DomainExpression<String>("1", Domain.singleValue(BIGINT, 1L));
        TupleExpression expression2Transformed = new DomainExpression<String>("2", Domain.singleValue(BIGINT, 2L));
        TupleExpression expression3Transformed = new DomainExpression<String>("3", Domain.singleValue(BIGINT, 3L));

        TupleExpression expectedExpression = new NotExpression<String>(
                new OrExpression<String>(expression3Transformed,
                        new AndExpression<Integer>(expression1Transformed, expression2Transformed)));

        assertEquals(transformed, expectedExpression);
    }

    @Test
    public void testExtractFixedValues()
            throws Exception
    {
        TupleExpression expression1 = new DomainExpression<ColumnHandle>(A, Domain.all(DOUBLE));
        TupleExpression expression2 = new DomainExpression<ColumnHandle>(B, Domain.singleValue(VARCHAR, utf8Slice("value")));
        TupleExpression expression3 = new DomainExpression<ColumnHandle>(C, Domain.onlyNull(BIGINT));
        TupleExpression expression4 = new DomainExpression<ColumnHandle>(D, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L)), true));
        TupleExpression combinedExpression = new AndExpression(new OrExpression(expression1, expression2),
                new OrExpression(expression3, expression4));
        assertEquals(
                combinedExpression.extractFixedValues(),
                ImmutableMap.of(
                        B, NullableValue.of(VARCHAR, utf8Slice("value")),
                        C, NullableValue.asNull(BIGINT)));
    }

    @Test
    public void testExtractFixedValuesFromNone()
            throws Exception
    {
        assertEquals(new NoneExpression<>().extractFixedValues(), ImmutableMap.of());
    }

    @Test
    public void testExtractFixedValuesFromAll()
            throws Exception
    {
        assertEquals(new AllExpression<>().extractFixedValues(), ImmutableMap.of());
    }
}
