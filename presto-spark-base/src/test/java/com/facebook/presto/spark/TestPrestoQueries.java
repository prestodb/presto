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
package com.facebook.presto.spark;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;

public class TestPrestoQueries
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner();
    }

    @Override
    public void testDescribeInput()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeInputNoParameters()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeInputWithAggregation()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeInputNoSuchQuery()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutput()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputNonSelect()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputShowTables()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputOnAliasedColumnsAndExpressions()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testDescribeOutputNoSuchQuery()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecute()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsing()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingWithDifferentDatatypes()
    {
        // prepared statement is not supported by Presto on Spark
    }

    public void testExecuteUsingComplexJoinCriteria()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingWithSubquery()
    {
        // prepared statement is not supported by Presto on Spark
    }

    public void testExecuteUsingWithSubqueryInJoin()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteWithParametersInGroupBy()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingFunction()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingSubqueryFails()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingSelectStarFails()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteUsingColumnReferenceFails()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainExecute()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainExecuteWithUsing()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainSetSessionWithUsing()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testPreparedStatementWithSubqueries()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExecuteWithParametersInLambda()
    {
        // prepared statement is not supported by Presto on Spark
    }

    @Override
    public void testExplainDdl()
    {
        // DDL statements are not supported by Presto on Spark
    }
}
