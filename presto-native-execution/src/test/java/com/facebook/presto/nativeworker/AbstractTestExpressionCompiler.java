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
package com.facebook.presto.nativeworker;

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.TestExpressionCompiler;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Ignore;

public abstract class AbstractTestExpressionCompiler
        extends TestExpressionCompiler
{
    @Override
    public FunctionAssertions setFunctionAssertions()
    {
        return new FunctionAssertions(getQueryRunner().getDefaultSession(), new FeaturesConfig().setNativeExecutionEnabled(true));
    }

    protected abstract QueryRunner getQueryRunner();

    // TODO: The following test have trouble converting long to Decimal.
    // https://github.com/prestodb/presto/issues/19999
    @Override
    @Ignore
    public void testBinaryOperatorsDecimalBigint()
            throws Exception
    {
    }

    @Override
    @Ignore
    public void testBinaryOperatorsDecimalInteger()
            throws Exception
    {
    }

    @Override
    @Ignore
    public void testBinaryOperatorsDecimalDouble()
            throws Exception
    {
    }

    // Remove the override from the following tests on a need basis, not all expressions have custom handling for native query runner, hence they are ignored.
    @Override
    @Ignore
    public void smokedTest()
    {
    }

    @Override
    @Ignore
    public void filterFunction()
    {
    }

    @Override
    @Ignore
    public void testUnaryOperators()
    {
    }

    @Override
    @Ignore
    public void testFilterEmptyInput()
    {
    }

    @Override
    @Ignore
    public void testNestedColumnFilter()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsBigintDecimal()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsBoolean()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsDoubleDecimal()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsDoubleDouble()
    {
    }

    public void testTernaryOperatorsLongLong()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsDoubleIntegral()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsIntegerDecimal()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsIntegralDouble()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsIntegralIntegral()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testBinaryOperatorsString()
    {
    }

    // Caused by: com.facebook.presto.spi.PrestoException: Catalog does not exist: hive
    @Override
    @Ignore
    public void testNullif()
    {
    }

    @Override
    @Ignore
    public void testTernaryOperatorsLongDouble()
    {
    }

    @Override
    @Ignore
    public void testTernaryOperatorsDoubleDouble()
    {
    }

    @Override
    @Ignore
    public void testTernaryOperatorsString()
    {
    }

    @Override
    @Ignore
    public void testTernaryOperatorsLongDecimal()
    {
    }

    @Override
    @Ignore
    public void testTernaryOperatorsDecimalDouble()
    {
    }

    @Override
    @Ignore
    public void testCast()
    {
    }

    @Override
    @Ignore
    public void testTryCast()
    {
    }

    @Override
    @Ignore
    public void testAnd()
    {
    }

    @Override
    @Ignore
    public void testOr()
    {
    }

    @Override
    @Ignore
    public void testNot()
    {
    }

    @Override
    @Ignore
    public void testIf()
    {
    }

    @Override
    @Ignore
    public void testSimpleCase()
    {
    }

    @Override
    @Ignore
    public void testSearchCaseSingle()
    {
    }

    @Override
    @Ignore
    public void testSearchCaseMultiple()
    {
    }

    @Override
    @Ignore
    public void testIn()
    {
    }

    @Override
    @Ignore
    public void testHugeIn()
    {
    }

    @Override
    @Ignore
    public void testInComplexTypes()
    {
    }

    @Override
    @Ignore
    public void testFunctionCall()
    {
    }

    @Override
    @Ignore
    public void testFunctionCallRegexp()
    {
    }

    @Override
    @Ignore
    public void testFunctionCallJson()
    {
    }

    @Override
    @Ignore
    public void testFunctionWithSessionCall()
    {
    }

    @Override
    @Ignore
    public void testExtract()
    {
    }

    @Override
    @Ignore
    public void testLike()
    {
    }

    @Override
    @Ignore
    public void testCoalesce()
    {
    }
}
