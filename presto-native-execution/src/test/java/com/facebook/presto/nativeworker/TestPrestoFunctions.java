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

import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.TestAggregations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPrestoFunctions
        extends AbstractTestHiveQueries
{
    public TestPrestoFunctions()
    {
        super(true);
    }

    static TestAggregations testAggregations;

    @BeforeClass
    protected void setQueryRunners()
    {
        QueryRunner queryRunner = super.getQueryRunner();
        ExpectedQueryRunner expectedQueryRunner = super.getExpectedQueryRunner();

        testAggregations = new TestAggregations();
        testAggregations.setQueryRunner(queryRunner);
        testAggregations.setExpectedQueryRunner(expectedQueryRunner);
    }

    @Test
    public void testAggregations()
    {
        testAggregations.testCountBoolean();
        testAggregations.testCountWithInlineView();
        testAggregations.testCountAllWithComparison();
        testAggregations.testCountWithNotPredicate();
        testAggregations.testCountWithNullPredicate();
        testAggregations.testAggregationPushdownThroughOuterJoinNotFiringInCorrelatedAggregatesLeftSide();
        testAggregations.testAggregationPushdownThroughOuterJoinNotFiringInCorrelatedAggregatesRightSide();
        testAggregations.testCountWithAndPredicate();
        testAggregations.testCountWithInlineView();
        testAggregations.testNestedCount();
        testAggregations.testGroupByOnSupersetOfPartitioning();
        testAggregations.testSumOfNulls();
        testAggregations.testCountAllWithPredicate();
        testAggregations.testGroupByArray();
        testAggregations.testGroupByMap();
        testAggregations.testGroupByComplexMap();
        testAggregations.testGroupByRow();
        testAggregations.testGroupByWithoutAggregation();
        testAggregations.testNestedGroupByWithSameKey();
        testAggregations.testGroupByWithNulls();
        testAggregations.testHistogram();
        testAggregations.testDistinctGroupBy();
        testAggregations.testDistinctWhere();
        testAggregations.testAggregationWithProjection();
        testAggregations.testSameInputToAggregates();
        testAggregations.testAggregationImplicitCoercion();
        testAggregations.testAggregationWithSomeArgumentCasts();
        testAggregations.testGroupByRepeatedField();
        testAggregations.testGroupByMultipleFieldsWithPredicateOnAggregationArgument();
        testAggregations.testReorderOutputsOfGroupByAggregation();
        testAggregations.testGroupAggregationOverNestedGroupByAggregation();
        testAggregations.testGroupByBetween();
        testAggregations.testGroupByOrdinal();
        testAggregations.testGroupBySearchedCase();
        testAggregations.testGroupBySearchedCaseNoElse();
        testAggregations.testGroupByIf();
        testAggregations.testGroupByCase();
        testAggregations.testGroupByCaseNoElse();
        testAggregations.testGroupByCast();
        testAggregations.testGroupByCoalesce();
        testAggregations.testGroupByNullConstant();
        testAggregations.test15WayGroupBy();
        testAggregations.testGroupByNoAggregations();
        testAggregations.testGroupByMultipleFields();
        testAggregations.testGroupBySum();
        testAggregations.testGroupByEmptyGroupingSet();
        testAggregations.testGroupByWithWildcard();
        testAggregations.testSingleGroupingSet();
        testAggregations.testGroupingSets();
        testAggregations.testGroupingSetsNoInput();
        testAggregations.testGroupingSetsWithSingleDistinct();
        testAggregations.testGroupingSetsGrandTotalSet();
        testAggregations.testGroupingSetsRepeatedSetsAll();
        testAggregations.testGroupingSetsRepeatedSetsDistinct();
        testAggregations.testGroupingSetsGrandTotalSetFirst();
        testAggregations.testGroupingSetsMultipleGrandTotalSets();
        testAggregations.testGroupingSetMixedExpressionAndColumn();
        testAggregations.testGroupingSetMixedExpressionAndOrdinal();
        testAggregations.testGroupingSetPredicatePushdown();
        testAggregations.testGroupingSetsAggregateOnGroupedColumn();
        testAggregations.testGroupingSetsMultipleAggregatesOnGroupedColumn();
        testAggregations.testGroupingSetsMultipleAggregatesOnUngroupedColumn();
        testAggregations.testGroupingSetsMultipleAggregatesWithGroupedColumns();
        testAggregations.testGroupingSetsWithSingleDistinctAndUnion();
        testAggregations.testGroupingSetsWithSingleDistinctAndUnionGroupedArguments();
        testAggregations.testRollup();
        testAggregations.testCube();

// TODO: Add tests for statistical functions
// TODO: Check/Fix the following aggregation tests that are failing
//        testAggregations.testCountWithIsNullPredicate();
//        testAggregations.testCountWithIsNotNullPredicate();
//        testAggregations.testCountWithNullIfPredicate();
//        testAggregations.testSingleDistinctOptimizer();
//        testAggregations.testExtractDistinctAggregationOptimizer();
//        testAggregations.testMultipleDifferentDistinct();
//        testAggregations.testMultipleDistinct();
//        testAggregations.testComplexDistinct();
//        testAggregations.testCountWithCoalescePredicate();
//        testAggregations.testCountDistinct();
//        testAggregations.testAggregationFilter();
//        testAggregations.testAggregationWithHaving();
//        testAggregations.testAggregationOverRightJoinOverSingleStreamProbe();
//        testAggregations.testAggregationPushedBelowOuterJoin();
//        testAggregations.testGroupByNullIf();
//        testAggregations.testGroupByExtract();
//        testAggregations.testApproximateCountDistinct();
//        testAggregations.testSumDataSizeForStats();
//        testAggregations.testMaxDataSizeForStats();
//        testAggregations.testApproximateCountDistinctGroupBy();
//        testAggregations.testApproximateCountDistinctGroupByWithStandardError();
//        testAggregations.testDistinctNan();
//        testAggregations.testGroupByNan();
//        testAggregations.testGroupByNanRow();
//        testAggregations.testGroupByNanArray();
//        testAggregations.testGroupByNanMap();
//        testAggregations.testGroupByCount();
//        testAggregations.testGroupByWithAlias();
//        testAggregations.testGroupByRequireIntegerCoercion();
//        testAggregations.testGroupingSetsWithGlobalAggregationNoInput();
//        testAggregations.testGroupingSetsWithMultipleDistinct();
//        testAggregations.testGroupingSetsWithMultipleDistinctNoInput();
//        testAggregations.testGroupingSetsRepeatedSetsAllNoInput();
//        testAggregations.testGroupingSetsMultipleGrandTotalSetsNoInput();
//        testAggregations.testGroupingSetsAliasedGroupingColumns();
//        testAggregations.testGroupingSetSubsetAndPartitioning();
//        testAggregations.testGroupingSetsWithMultipleDistinctAndUnion();
//        testAggregations.testCubeNoInput();
//        testAggregations.testGroupingCombinationsAll();
//        testAggregations.testGroupingCombinationsDistinct();
//        testAggregations.testOrderedAggregations();
//        testAggregations.testGroupedRow();
    }
}
