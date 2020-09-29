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

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.gen.InCodeGenerator.SwitchGenerationCase.DIRECT_SWITCH;
import static com.facebook.presto.sql.gen.InCodeGenerator.SwitchGenerationCase.HASH_SWITCH;
import static com.facebook.presto.sql.gen.InCodeGenerator.SwitchGenerationCase.SET_CONTAINS;
import static com.facebook.presto.sql.gen.InCodeGenerator.checkSwitchGenerationCase;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static org.testng.Assert.assertEquals;

public class TestInCodeGenerator
{
    @Test
    public void testInteger()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestMetadataManager().getFunctionAndTypeManager();
        List<RowExpression> values = new ArrayList<>();
        values.add(constant((long) Integer.MIN_VALUE, INTEGER));
        values.add(constant((long) Integer.MAX_VALUE, INTEGER));
        values.add(constant(3L, INTEGER));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);

        values.add(constant(null, INTEGER));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);
        values.add(new CallExpression(
                CAST.name(),
                functionAndTypeManager.lookupCast(CAST, DOUBLE.getTypeSignature(), INTEGER.getTypeSignature()),
                INTEGER,
                Collections.singletonList(constant(12345678901234.0, DOUBLE))));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);

        for (int i = 6; i <= 32; ++i) {
            values.add(constant((long) i, INTEGER));
        }
        assertEquals(checkSwitchGenerationCase(INTEGER, values), DIRECT_SWITCH);

        values.add(constant(33L, INTEGER));
        assertEquals(checkSwitchGenerationCase(INTEGER, values), SET_CONTAINS);
    }

    @Test
    public void testBigint()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestMetadataManager().getFunctionAndTypeManager();
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(Integer.MAX_VALUE + 1L, BIGINT));
        values.add(constant(Integer.MIN_VALUE - 1L, BIGINT));
        values.add(constant(3L, BIGINT));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);

        values.add(constant(null, BIGINT));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);
        values.add(new CallExpression(
                CAST.name(),
                functionAndTypeManager.lookupCast(CAST, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()),
                BIGINT,
                Collections.singletonList(constant(12345678901234.0, DOUBLE))));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);

        for (long i = 6; i <= 32; ++i) {
            values.add(constant(i, BIGINT));
        }
        assertEquals(checkSwitchGenerationCase(BIGINT, values), HASH_SWITCH);

        values.add(constant(33L, BIGINT));
        assertEquals(checkSwitchGenerationCase(BIGINT, values), SET_CONTAINS);
    }

    @Test
    public void testDate()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(1L, DATE));
        values.add(constant(2L, DATE));
        values.add(constant(3L, DATE));
        assertEquals(checkSwitchGenerationCase(DATE, values), DIRECT_SWITCH);

        for (long i = 4; i <= 32; ++i) {
            values.add(constant(i, DATE));
        }
        assertEquals(checkSwitchGenerationCase(DATE, values), DIRECT_SWITCH);

        values.add(constant(33L, DATE));
        assertEquals(checkSwitchGenerationCase(DATE, values), SET_CONTAINS);
    }

    @Test
    public void testDouble()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(1.5, DOUBLE));
        values.add(constant(2.5, DOUBLE));
        values.add(constant(3.5, DOUBLE));
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), HASH_SWITCH);

        values.add(constant(null, DOUBLE));
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), HASH_SWITCH);

        for (int i = 5; i <= 32; ++i) {
            values.add(constant(i + 0.5, DOUBLE));
        }
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), HASH_SWITCH);

        values.add(constant(33.5, DOUBLE));
        assertEquals(checkSwitchGenerationCase(DOUBLE, values), SET_CONTAINS);
    }

    @Test
    public void testVarchar()
    {
        List<RowExpression> values = new ArrayList<>();
        values.add(constant(Slices.utf8Slice("1"), VARCHAR));
        values.add(constant(Slices.utf8Slice("2"), VARCHAR));
        values.add(constant(Slices.utf8Slice("3"), VARCHAR));
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), HASH_SWITCH);

        values.add(constant(null, VARCHAR));
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), HASH_SWITCH);

        for (int i = 5; i <= 32; ++i) {
            values.add(constant(Slices.utf8Slice(String.valueOf(i)), VARCHAR));
        }
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), HASH_SWITCH);

        values.add(constant(Slices.utf8Slice("33"), VARCHAR));
        assertEquals(checkSwitchGenerationCase(VARCHAR, values), SET_CONTAINS);
    }
}
