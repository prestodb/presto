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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.operator.scalar.annotations.SqlInvokedScalarFromAnnotationsParser;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestCustomFunctions
        extends AbstractTestFunctions
{
    public TestCustomFunctions()
    {
    }

    protected TestCustomFunctions(FeaturesConfig config)
    {
        super(config);
    }

    @BeforeClass
    public void setupClass()
    {
        registerScalar(CustomFunctions.class);
        List<SqlInvokedFunction> functions = SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinitions(CustomFunctions.class);
        this.functionAssertions.addFunctions(functions);
    }

    @Test
    public void testCustomAdd()
    {
        assertFunction("custom_add(123, 456)", BIGINT, 579L);
    }

    @Test
    public void testSliceIsNull()
    {
        assertFunction("custom_is_null(CAST(NULL AS VARCHAR))", BOOLEAN, true);
        assertFunction("custom_is_null('not null')", BOOLEAN, false);
    }

    @Test
    public void testLongIsNull()
    {
        assertFunction("custom_is_null(CAST(NULL AS BIGINT))", BOOLEAN, true);
        assertFunction("custom_is_null(0)", BOOLEAN, false);
    }

    @Test
    public void testNullIf()
    {
        assertFunction("custom_square(2, 5)", IntegerType.INTEGER, 4);
        assertFunction("custom_square(5, 5)", IntegerType.INTEGER, 25);
    }
}
