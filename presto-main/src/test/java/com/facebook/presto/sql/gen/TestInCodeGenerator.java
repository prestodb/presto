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
package com.facebook.presto.sql.gen;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.RowExpression;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.gen.InCodeGenerator.SwitchGenerationCase.DIRECT_SWITCH;
import static com.facebook.presto.sql.gen.InCodeGenerator.SwitchGenerationCase.HASH_SWITCH;
import static com.facebook.presto.sql.gen.InCodeGenerator.SwitchGenerationCase.SET_CONTAINS;
import static com.facebook.presto.sql.gen.InCodeGenerator.checkSwitchGenerationCase;
import static com.facebook.presto.sql.relational.Signatures.CAST;
import static org.testng.Assert.assertEquals;

public class TestInCodeGenerator
{
    @Test
    public void testInteger()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(new ConstantExpression(Integer.MIN_VALUE, INTEGER));
        values.add(new ConstantExpression(Integer.MAX_VALUE, INTEGER));
        values.add(new ConstantExpression(3, INTEGER));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);

        values.add(new ConstantExpression(null, INTEGER));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);
        values.add(new CallExpression(
                new Signature(
                        CAST,
                        SCALAR,
                        INTEGER.getTypeSignature(),
                        DOUBLE.getTypeSignature()
                ),
                INTEGER,
                Collections.singletonList(new ConstantExpression(12345678901234.0, DOUBLE))
        ));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);

        for  (int i = 6; i <= 32; ++i) {
            values.add(new ConstantExpression(i, INTEGER));
        }
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);

        values.add(new ConstantExpression(33, INTEGER));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), SET_CONTAINS);
    }

    @Test
    public void testBigint()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(new ConstantExpression(Integer.MAX_VALUE + 1L, BIGINT));
        values.add(new ConstantExpression(Integer.MIN_VALUE - 1L, BIGINT));
        values.add(new ConstantExpression(3L, BIGINT));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);

        values.add(new ConstantExpression(null, BIGINT));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);
        values.add(new CallExpression(
                new Signature(
                        CAST,
                        SCALAR,
                        BIGINT.getTypeSignature(),
                        DOUBLE.getTypeSignature()
                ),
                BIGINT,
                Collections.singletonList(new ConstantExpression(12345678901234.0, DOUBLE))
        ));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);

        for  (long i = 6; i <= 32; ++i) {
            values.add(new ConstantExpression(i, BIGINT));
        }
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);

        values.add(new ConstantExpression(33L, BIGINT));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), SET_CONTAINS);
    }

    @Test
    public void testDate()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(new ConstantExpression(1L, DATE));
        values.add(new ConstantExpression(2L, DATE));
        values.add(new ConstantExpression(3L, DATE));
        assertEquals(checkSwitchGenerationCase(DATE, values), DIRECT_SWITCH);

        for  (long i = 4; i <= 32; ++i) {
            values.add(new ConstantExpression(i, DATE));
        }
        assertEquals(checkSwitchGenerationCase(DATE, values), DIRECT_SWITCH);

        values.add(new ConstantExpression(33L, DATE));
        assertEquals(checkSwitchGenerationCase(DATE, values), SET_CONTAINS);
    }

    @Test
    public void testDouble()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(new ConstantExpression(1.5, DOUBLE));
        values.add(new ConstantExpression(2.5, DOUBLE));
        values.add(new ConstantExpression(3.5, DOUBLE));
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), HASH_SWITCH);

        values.add(new ConstantExpression(null, DOUBLE));
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), HASH_SWITCH);

        for  (int i = 5; i <= 32; ++i) {
            values.add(new ConstantExpression(i + 0.5, DOUBLE));
        }
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), HASH_SWITCH);

        values.add(new ConstantExpression(33.5, DOUBLE));
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), SET_CONTAINS);
    }

    @Test
    public void testVarchar()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(new ConstantExpression(Slices.utf8Slice("1"), DOUBLE));
        values.add(new ConstantExpression(Slices.utf8Slice("2"), DOUBLE));
        values.add(new ConstantExpression(Slices.utf8Slice("3"), DOUBLE));
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), HASH_SWITCH);

        values.add(new ConstantExpression(null, VARCHAR));
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), HASH_SWITCH);

        for  (int i = 5; i <= 32; ++i) {
            values.add(new ConstantExpression(Slices.utf8Slice(String.valueOf(i)), VARCHAR));
        }
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), HASH_SWITCH);

        values.add(new ConstantExpression(Slices.utf8Slice("33"), VARCHAR));
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), SET_CONTAINS);
    }
}
