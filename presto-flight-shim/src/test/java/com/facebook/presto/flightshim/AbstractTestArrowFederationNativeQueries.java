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
package com.facebook.presto.flightshim;

import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public abstract class AbstractTestArrowFederationNativeQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @DataProvider(name = "use_default_literal_coalesce")
    public static Object[][] useDefaultLiteralCoalesce()
    {
        return new Object[][] {{true}};
    }

    @Override
    @DataProvider(name = "optimize_hash_generation")
    public Object[][] optimizeHashGeneration()
    {
        return new Object[][] {{"false"}};
    }

    @Test
    public void testBasic()
    {
        assertQuery("select 1 + 2");
        assertQuery("select * from nation");
        assertQuery("select * from nation where nationkey between 10 and 20");
    }

    @Override
    @Test
    public void testArrayCumSumVarchar()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testApplyLambdaRepeated()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testCustomAdd()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLambdaCapture()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLambdaInAggregationContext()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLambdaInSubqueryContext()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMapBlockBug()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testNonDeterministicInLambda()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testRowSubscriptInLambda()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testTryWithLambda()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLog()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testCustomSum()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testCustomRank()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLikePrefixAndSuffixWithChars()
    {
        // type not present under native execution
    }

    @Override
    @Test
    public void testMergeKHyperLogLog()
    {
        // type not present under native execution
    }

    @Override
    @Test
    public void testStringFilters()
    {
        // function not available under native execution
    }

    // todo: Add support for INSERT AND CTAS Statements.

    @Override
    @Test
    public void testAddColumn()
    {
    }

    @Override
    @Test
    public void testDropColumn()
    {
    }

    @Override
    @Test
    public void testQueryLoggingCount()
    {
    }

    @Override
    @Test
    public void testRenameColumn()
    {
    }

    @Override
    @Test
    public void testRenameTable()
    {
    }

    @Override
    @Test
    public void testCreateTableAsSelect()
    {
    }

    @Override
    @Test
    public void testInsertIntoNotNullColumn()
    {
    }

    @Override
    @Test
    public void testSymbolAliasing()
    {
    }

    @Override
    @Test
    public void testWrittenStats()
    {
    }

    @Override
    @Test
    public void testCompatibleTypeChangeForView()
    {
    }

    @Override
    @Test
    public void testCompatibleTypeChangeForView2()
    {
    }

    /// NOTE: These test cases need some modifications to run on Presto C++ which will be addressed in the PR : https://github.com/prestodb/presto/pull/23671.
    /// Once that PR is merged, this class should be updated to inherit from AbstractTestQueriesNative and the temporary changes made here can be reverted
    // todo: hack starts
    @Override
    @Test
    public void testReduceAgg()
    {
    }

    /// This test is not applicable in Presto C++ since there is no bytecode IR as with JVM.
    @Override
    @Test(enabled = false)
    public void testLargeBytecode() {}

    @Override
    @Test
    public void testDuplicateUnnestRows()
    {
    }

    @Override
    @Test
    public void testSetUnionIndeterminateRows()
    {
    }

    @Override
    @Test
    public void testRemoveMapCastFailure()
    {
    }

    @Override
    @Test
    public void testSameAggregationWithAndWithoutFilter()
    {
    }

    @Override
    @Test
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
    }

    @Override
    @Test
    public void testScalarSubquery()
    {
    }

    @Override
    @Test
    public void testComplexCast()
    {
    }

    @Override
    @Test
    public void testRunawayRegexAnalyzerTimeout()
    {
    }

    @Override
    @Test
    public void testLambdaInAggregation()
    {
    }

    @Override
    @Test
    public void testReduceAggWithNulls()
    {
    }
    // hack ends

    // Investigate the failures below
    @Test(enabled = false)
    public void testAssignUniqueId()
    {
    }
    @Test(enabled = false)
    public void testTopNUnpartitionedWindow()
    {
    }
}
