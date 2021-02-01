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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.MODULUS;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.function.OperatorType.NEGATION;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.function.OperatorType.SUBTRACT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;

public class TestRowExpressionFormatter
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final RowExpressionFormatter FORMATTER = new RowExpressionFormatter(FUNCTION_AND_TYPE_MANAGER);
    private static final VariableReferenceExpression C_BIGINT = new VariableReferenceExpression("c_bigint", BIGINT);
    private static final VariableReferenceExpression C_BIGINT_ARRAY = new VariableReferenceExpression("c_bigint_array", new ArrayType(BIGINT));

    @Test
    public void testConstants()
    {
        // null
        RowExpression constantExpression = constantNull(UNKNOWN);
        assertEquals(format(constantExpression), "null");

        // boolean
        constantExpression = constant(true, BOOLEAN);
        assertEquals(format(constantExpression), "BOOLEAN true");

        // double
        constantExpression = constant(1.1, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE 1.1");
        constantExpression = constant(Double.NaN, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE NaN");
        constantExpression = constant(Double.POSITIVE_INFINITY, DOUBLE);
        assertEquals(format(constantExpression), "DOUBLE Infinity");

        // real
        constantExpression = constant((long) floatToIntBits(1.1f), REAL);
        assertEquals(format(constantExpression), "REAL 1.1");
        constantExpression = constant((long) floatToIntBits(Float.NaN), REAL);
        assertEquals(format(constantExpression), "REAL NaN");
        constantExpression = constant((long) floatToIntBits(Float.POSITIVE_INFINITY), REAL);
        assertEquals(format(constantExpression), "REAL Infinity");

        // string
        constantExpression = constant(Slices.utf8Slice("abcde"), VARCHAR);
        assertEquals(format(constantExpression), "VARCHAR abcde");
        constantExpression = constant(Slices.utf8Slice("fgh"), createCharType(3));
        assertEquals(format(constantExpression), "CHAR(3) fgh");

        // integer
        constantExpression = constant(1L, TINYINT);
        assertEquals(format(constantExpression), "TINYINT 1");
        constantExpression = constant(1L, SMALLINT);
        assertEquals(format(constantExpression), "SMALLINT 1");
        constantExpression = constant(1L, INTEGER);
        assertEquals(format(constantExpression), "INTEGER 1");
        constantExpression = constant(1L, BIGINT);
        assertEquals(format(constantExpression), "BIGINT 1");

        // varbinary
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456"));
        constantExpression = constant(value, VARBINARY);
        assertEquals(format(constantExpression), "VARBINARY 12 34 56");

        // color
        constantExpression = constant(256L, COLOR);
        assertEquals(format(constantExpression), "COLOR 256");

        // long and short decimals
        constantExpression = constant(decimal("1.2345678910"), DecimalType.createDecimalType(11, 10));
        assertEquals(format(constantExpression), "DECIMAL(11,10) 1.2345678910");
        constantExpression = constant(decimal("1.281734081274028174012432412423134"), DecimalType.createDecimalType(34, 33));
        assertEquals(format(constantExpression), "DECIMAL(34,33) 1.281734081274028174012432412423134");

        // time
        constantExpression = constant(662727600000L, TIMESTAMP);
        assertEquals(format(constantExpression), "TIMESTAMP 1991-01-01 00:00:00.000");
        constantExpression = constant(7670L, DATE);
        assertEquals(format(constantExpression), "DATE 1991-01-01");

        // interval
        constantExpression = constant(24L, INTERVAL_DAY_TIME);
        assertEquals(format(constantExpression), "INTERVAL DAY TO SECOND 0 00:00:00.024");
        constantExpression = constant(25L, INTERVAL_YEAR_MONTH);
        assertEquals(format(constantExpression), "INTERVAL YEAR TO MONTH 2-1");

        // block
        constantExpression = constant(new LongArrayBlockBuilder(null, 4).writeLong(1L).writeLong(2).build(), new ArrayType(BIGINT));
        assertEquals(format(constantExpression), "[Block: position count: 2; size: 96 bytes]");
    }

    @Test
    public void testCalls()
    {
        RowExpression callExpression;

        // arithmetic
        callExpression = createCallExpression(ADD);
        assertEquals(format(callExpression), "(c_bigint) + (BIGINT 5)");
        callExpression = createCallExpression(SUBTRACT);
        assertEquals(format(callExpression), "(c_bigint) - (BIGINT 5)");
        callExpression = createCallExpression(MULTIPLY);
        assertEquals(format(callExpression), "(c_bigint) * (BIGINT 5)");
        callExpression = createCallExpression(DIVIDE);
        assertEquals(format(callExpression), "(c_bigint) / (BIGINT 5)");
        callExpression = createCallExpression(MODULUS);
        assertEquals(format(callExpression), "(c_bigint) % (BIGINT 5)");

        // comparison
        callExpression = createCallExpression(GREATER_THAN);
        assertEquals(format(callExpression), "(c_bigint) > (BIGINT 5)");
        callExpression = createCallExpression(LESS_THAN);
        assertEquals(format(callExpression), "(c_bigint) < (BIGINT 5)");
        callExpression = createCallExpression(GREATER_THAN_OR_EQUAL);
        assertEquals(format(callExpression), "(c_bigint) >= (BIGINT 5)");
        callExpression = createCallExpression(LESS_THAN_OR_EQUAL);
        assertEquals(format(callExpression), "(c_bigint) <= (BIGINT 5)");
        callExpression = createCallExpression(EQUAL);
        assertEquals(format(callExpression), "(c_bigint) = (BIGINT 5)");
        callExpression = createCallExpression(NOT_EQUAL);
        assertEquals(format(callExpression), "(c_bigint) <> (BIGINT 5)");
        callExpression = createCallExpression(IS_DISTINCT_FROM);
        assertEquals(format(callExpression), "(c_bigint) IS DISTINCT FROM (BIGINT 5)");

        // negation
        RowExpression expression = createCallExpression(ADD);
        callExpression = call(
                NEGATION.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(NEGATION, fromTypes(expression.getType())),
                expression.getType(),
                expression);
        assertEquals(format(callExpression), "-((c_bigint) + (BIGINT 5))");

        // subscript
        ArrayType arrayType = (ArrayType) C_BIGINT_ARRAY.getType();
        Type elementType = arrayType.getElementType();
        RowExpression subscriptExpression = call(SUBSCRIPT.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(SUBSCRIPT, fromTypes(arrayType, elementType)),
                elementType,
                ImmutableList.of(C_BIGINT_ARRAY, constant(0L, INTEGER)));
        callExpression = subscriptExpression;
        assertEquals(format(callExpression), "c_bigint_array[INTEGER 0]");

        // cast
        callExpression = call(
            CAST.name(),
            FUNCTION_AND_TYPE_MANAGER.lookupCast(CastType.CAST, TINYINT.getTypeSignature(), BIGINT.getTypeSignature()),
            BIGINT,
            constant(1L, TINYINT));
        assertEquals(format(callExpression), "CAST(TINYINT 1 AS bigint)");

        // between
        callExpression = call(
                BETWEEN.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(BETWEEN, fromTypes(BIGINT, BIGINT, BIGINT)),
                BOOLEAN,
                subscriptExpression,
                constant(1L, BIGINT),
                constant(5L, BIGINT));
        assertEquals(format(callExpression), "c_bigint_array[INTEGER 0] BETWEEN (BIGINT 1) AND (BIGINT 5)");

        // other
        callExpression = call(
                HASH_CODE.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(HASH_CODE, fromTypes(BIGINT)),
                BIGINT,
                constant(1L, BIGINT));
        assertEquals(format(callExpression), "HASH_CODE(BIGINT 1)");
    }

    @Test
    public void testSpecialForm()
    {
        RowExpression specialFormExpression;

        // or and and
        specialFormExpression = new SpecialFormExpression(OR, BOOLEAN, createCallExpression(NOT_EQUAL), createCallExpression(IS_DISTINCT_FROM));
        assertEquals(format(specialFormExpression), "((c_bigint) <> (BIGINT 5)) OR ((c_bigint) IS DISTINCT FROM (BIGINT 5))");
        specialFormExpression = new SpecialFormExpression(AND, BOOLEAN, createCallExpression(EQUAL), createCallExpression(GREATER_THAN));
        assertEquals(format(specialFormExpression), "((c_bigint) = (BIGINT 5)) AND ((c_bigint) > (BIGINT 5))");

        // other
        specialFormExpression = new SpecialFormExpression(IS_NULL, BOOLEAN, createCallExpression(ADD));
        assertEquals(format(specialFormExpression), "IS_NULL((c_bigint) + (BIGINT 5))");
    }

    @Test
    public void testComplex()
    {
        RowExpression complexExpression;

        RowExpression expression = createCallExpression(ADD);
        complexExpression = call(
                SUBTRACT.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(SUBTRACT, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                C_BIGINT,
                expression);
        assertEquals(format(complexExpression), "(c_bigint) - ((c_bigint) + (BIGINT 5))");

        RowExpression expression1 = createCallExpression(ADD);
        RowExpression expression2 = call(
                MULTIPLY.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(MULTIPLY, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                expression1,
                C_BIGINT);
        RowExpression expression3 = createCallExpression(GREATER_THAN);
        complexExpression = new SpecialFormExpression(OR, BOOLEAN, expression2, expression3);
        assertEquals(format(complexExpression), "(((c_bigint) + (BIGINT 5)) * (c_bigint)) OR ((c_bigint) > (BIGINT 5))");

        ArrayType arrayType = (ArrayType) C_BIGINT_ARRAY.getType();
        Type elementType = arrayType.getElementType();
        expression1 = call(SUBSCRIPT.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(SUBSCRIPT, fromTypes(arrayType, elementType)),
                elementType,
                ImmutableList.of(C_BIGINT_ARRAY, constant(5L, INTEGER)));
        expression2 = call(
                NEGATION.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(NEGATION, fromTypes(expression1.getType())),
                expression1.getType(),
                expression1);
        expression3 = call(
                ADD.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(ADD, fromTypes(expression2.getType(), BIGINT)),
                BIGINT,
                expression2,
                constant(5L, BIGINT));
        assertEquals(format(expression3), "(-(c_bigint_array[INTEGER 5])) + (BIGINT 5)");
    }

    protected static Object decimal(String decimalString)
    {
        return Decimals.parseIncludeLeadingZerosInPrecision(decimalString).getObject();
    }

    private static CallExpression createCallExpression(OperatorType type)
    {
        return call(
                type.name(),
                FUNCTION_AND_TYPE_MANAGER.resolveOperator(type, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                C_BIGINT,
                constant(5L, BIGINT));
    }

    private static String format(RowExpression expression)
    {
        return FORMATTER.formatRowExpression(TEST_SESSION.toConnectorSession(), expression);
    }
}
