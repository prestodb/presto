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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.metastore.Column;
import com.google.common.base.Strings;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.metastore.glue.GlueExpressionUtil.GLUE_EXPRESSION_CHAR_LIMIT;
import static com.facebook.presto.hive.metastore.glue.GlueExpressionUtil.buildGlueExpression;
import static com.facebook.presto.hive.metastore.glue.GlueExpressionUtil.buildGlueExpressionForSingleDomain;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestGlueExpressionUtil
{
    private static final TypeTranslator HIVE_TYPE_TRANSLATOR = new HiveTypeTranslator();

    @Test
    public void testBuildGlueExpressionDomainEqualsSingleValue()
    {
        Domain domain = Domain.singleValue(VarcharType.VARCHAR, utf8Slice("2020-01-01"));
        Optional<String> foo = buildGlueExpressionForSingleDomain("foo", domain);
        assertEquals(foo.get(), "((foo = '2020-01-01'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualsSingleValue()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues("col1", "2020-01-01")
                .addStringValues("col2", "2020-02-20")
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col1 = '2020-01-01')) AND ((col2 = '2020-02-20'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualsAndInClause()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues("col1", "2020-01-01")
                .addStringValues("col2", "2020-02-20", "2020-02-28")
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col1 = '2020-01-01')) AND ((col2 in ('2020-02-20', '2020-02-28')))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainRange()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues("col1", "2020-01-01")
                .addRanges("col2", Range.greaterThan(BIGINT, 100L))
                .addRanges("col2", Range.lessThan(BIGINT, 0L))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col1 = '2020-01-01')) AND ((col2 < 0) OR (col2 > 100))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualAndRangeLong()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addBigintValues("col1", 3L)
                .addRanges("col1", Range.greaterThan(BIGINT, 100L))
                .addRanges("col1", Range.lessThan(BIGINT, 0L))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col1 < 0) OR (col1 > 100) OR (col1 = 3))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualAndRangeString()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues("col1", "2020-01-01", "2020-01-31")
                .addRanges("col1", Range.range(VarcharType.VARCHAR, utf8Slice("2020-03-01"), true, utf8Slice("2020-03-31"), true))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col1 >= '2020-03-01' AND col1 <= '2020-03-31') OR (col1 in ('2020-01-01', '2020-01-31')))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainIsNull()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain("col1", Domain.onlyNull(VarcharType.VARCHAR))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, format("(col1 = '%s')", HIVE_DEFAULT_DYNAMIC_PARTITION));
    }

    @Test
    public void testBuildGlueExpressionTupleDomainNotNull()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDomain("col1", Domain.notNull(VarcharType.VARCHAR))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, format("(col1 <> '%s')", HIVE_DEFAULT_DYNAMIC_PARTITION));
    }

    @Test
    public void testBuildGlueExpressionMaxLengthNone()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues("col1", Strings.repeat("x", GLUE_EXPRESSION_CHAR_LIMIT))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "");
    }

    @Test
    public void testBuildGlueExpressionMaxLengthOneColumn()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addStringValues("col1", Strings.repeat("x", GLUE_EXPRESSION_CHAR_LIMIT))
                .addStringValues("col2", Strings.repeat("x", 5))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col2 = 'xxxxx'))");
    }

    @Test
    public void testDecimalConversion()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addDecimalValues("col1", "10.134")
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, "((col1 = 10.13400))");
    }

    @Test
    public void testBigintConversion()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addBigintValues("col1", Long.MAX_VALUE)
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, format("((col1 = %d))", Long.MAX_VALUE));
    }

    @Test
    public void testIntegerConversion()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addIntegerValues("col1", Long.valueOf(Integer.MAX_VALUE))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, format("((col1 = %d))", Integer.MAX_VALUE));
    }

    @Test
    public void testSmallintConversion()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addIntegerValues("col1", Long.valueOf(Short.MAX_VALUE))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, format("((col1 = %d))", Short.MAX_VALUE));
    }

    @Test
    public void testTinyintConversion()
    {
        Map<Column, Domain> predicates = new PartitionFilterBuilder(HIVE_TYPE_TRANSLATOR)
                .addIntegerValues("col1", Long.valueOf(Byte.MAX_VALUE))
                .build();
        String expression = buildGlueExpression(predicates);
        assertEquals(expression, format("((col1 = %d))", Byte.MAX_VALUE));
    }
}
