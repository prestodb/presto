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
package com.facebook.presto.sql;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.HyperLogLogType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.relational.RowExpressionToSqlTranslator;
import com.facebook.presto.sql.relational.StandardFunctionResolution;
import com.facebook.presto.type.JsonType;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestRowExpressionToSqlTranslator
{
    private Metadata metadata;
    private StandardFunctionResolution standardFunctionResolution;

    @BeforeClass
    public void setup()
    {
        metadata = createTestMetadataManager();
        standardFunctionResolution = new StandardFunctionResolution(metadata.getFunctionManager());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
    }

    @Test
    public void testToPredicate()
    {
        RowExpression rowExpression;

        List<RowExpression> values = new ArrayList<>();
        values.add(new VariableReferenceExpression("a", BigintType.BIGINT));
        values.add(new ConstantExpression(2L, BigintType.BIGINT));
        rowExpression = new SpecialFormExpression(SpecialFormExpression.Form.IN, BooleanType.BOOLEAN, values);
        assertEquals(RowExpressionToSqlTranslator.translate(rowExpression).toString(), "(a IN (2))");

        values.clear();
        values.add(new VariableReferenceExpression("a", BigintType.BIGINT));
        values.add(new ConstantExpression(2L, BigintType.BIGINT));
        values.add(new ConstantExpression(3L, BigintType.BIGINT));
        rowExpression = new SpecialFormExpression(SpecialFormExpression.Form.IN, BooleanType.BOOLEAN, values);
        assertEquals(RowExpressionToSqlTranslator.translate(rowExpression).toString(), "(a IN (2, 3))");

        values.clear();
        values.add(new VariableReferenceExpression("a", BigintType.BIGINT));
        values.add(new ConstantExpression(2L, BigintType.BIGINT));
        values.add(new ConstantExpression(3L, BigintType.BIGINT));
        values.add(new ConstantExpression(4, IntegerType.INTEGER));
        rowExpression = new SpecialFormExpression(SpecialFormExpression.Form.IN, BooleanType.BOOLEAN, values);
        assertEquals(RowExpressionToSqlTranslator.translate(rowExpression).toString(), "(a IN (2, 3, 4))");

        values.clear();
        values.add(new VariableReferenceExpression("a", BigintType.BIGINT));
        values.add(new ConstantExpression(2L, BigintType.BIGINT));
        values.add(new ConstantExpression(3L, BigintType.BIGINT));
        values.add(new ConstantExpression("4L", VarcharType.VARCHAR));
        RowExpression rowExpression2 = new SpecialFormExpression(SpecialFormExpression.Form.IN, BooleanType.BOOLEAN, values);
        // Although the conversion will pass, the result will be caught during semantic analysis
        assertEquals(RowExpressionToSqlTranslator.translate(rowExpression).toString(), "(a IN (2, 3, '4L'))");

        RowExpression binaryRowExpression = specialForm(SpecialFormExpression.Form.AND, BooleanType.BOOLEAN, rowExpression2, rowExpression);
        assertThrows(() -> RowExpressionToSqlTranslator.translate(binaryRowExpression));

        RowExpression callExpression = new CallExpression(
                "not",
                standardFunctionResolution.notFunction(),
                BOOLEAN,
                ImmutableList.of(rowExpression));
        assertThrows(() -> RowExpressionToSqlTranslator.translate(callExpression));
    }

    @Test
    public void testSupportedTypes()
    {
        RowExpressionToSqlTranslator.translate(new ConstantExpression(null, UnknownType.UNKNOWN));
        RowExpressionToSqlTranslator.translate(new ConstantExpression(true, BooleanType.BOOLEAN));
        RowExpressionToSqlTranslator.translate(new ConstantExpression(2, IntegerType.INTEGER));
        RowExpressionToSqlTranslator.translate(new ConstantExpression(2L, BigintType.BIGINT));
        RowExpressionToSqlTranslator.translate(new ConstantExpression(3.142, DoubleType.DOUBLE));
        RowExpressionToSqlTranslator.translate(new ConstantExpression("abcd", VarcharType.VARCHAR));
        RowExpressionToSqlTranslator.translate(new ConstantExpression(90061L, DateType.DATE));
        RowExpressionToSqlTranslator.translate(new ConstantExpression(1555921472L, TimestampType.TIMESTAMP));
    }

    @Test
    public void testUnsupportedTypes()
    {
        Object dummy = "abcd"; // we don't have to initialize
        assertThrows(() -> RowExpressionToSqlTranslator.translate(new ConstantExpression(dummy, JsonType.JSON)));
        assertThrows(() -> RowExpressionToSqlTranslator.translate(new ConstantExpression(dummy, VarbinaryType.VARBINARY)));
        assertThrows(() -> RowExpressionToSqlTranslator.translate(new ConstantExpression(dummy, HyperLogLogType.HYPER_LOG_LOG)));
        assertThrows(() -> RowExpressionToSqlTranslator.translate(new ConstantExpression(dummy, SmallintType.SMALLINT)));
        assertThrows(() -> RowExpressionToSqlTranslator.translate(new ConstantExpression(dummy, TinyintType.TINYINT)));
        assertThrows(() -> RowExpressionToSqlTranslator.translate(new ConstantExpression(dummy, RealType.REAL)));
    }
}
