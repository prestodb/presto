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
package com.facebook.presto.hive;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.hive.TestHiveEventListenerPlugin.TestingHiveEventListener;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveClustering
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(getTables());
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void close()
            throws Exception
    {
        Optional<EventListener> eventListener = getQueryRunner().getEventListener();
        assertTrue(eventListener.isPresent());
        assertTrue(eventListener.get() instanceof TestingHiveEventListener);

        Set<QueryId> runningQueryIds = ((TestingHiveEventListener) eventListener.get()).getRunningQueries();

        TimeUnit.MILLISECONDS.sleep(500);
        Logger logger = Logger.getLogger(TestHiveClustering.class.getName());
        logger.info("Running queries" + runningQueryIds.toString());
        assertTrue(runningQueryIds.isEmpty(), format(
                "Query completion events not sent for %d queries",
                runningQueryIds.size()));

        super.close();
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_copied \n" +
                        " WITH (" +
                        "    format = 'ORC',\n" +
                        "    clustered_by = array['custkey'], \n" +
                        "    cluster_count = array[10], \n" +
                        "    distribution = array['150', '300', '450', '600', '750', '900', '1050', '1200', '1350']) \n" +
                        " AS SELECT * FROM customer");

        MaterializedResult result = computeActual(query);
        assertEquals(result.getOnlyValue(), 1500L);

        query = "SELECT custkey, COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_copied GROUP BY custkey ORDER BY custkey";
        result = computeActual(query);
        assertEquals(result.getRowCount(), 1500L);

        query = "SELECT name FROM hive_bucketed.tpch_bucketed.customer_copied WHERE custkey <= 100";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        QueryInfo queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                10.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                10.0);
        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 100L);

        query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_copied_again \n" +
                        " WITH (" +
                        "    format = 'ORC',\n" +
                        "    bucketed_by = array['custkey'], \n" +
                        "    bucket_count = 11) \n" +
                        " AS SELECT * FROM customer");

        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 1500L);

        query = "SELECT name FROM hive_bucketed.tpch_bucketed.customer_copied_again WHERE custkey <= 100";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                100.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                100.0);
        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 100L);
    }

    @Test
    public void testCreateTableAndInsert()
    {
        String query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_created (\n" +
                        "    custkey bigint,\n" +
                        "    name varchar(25),\n" +
                        "    address varchar(40),\n" +
                        "    nationkey bigint,\n" +
                        "    phone varchar(15),\n" +
                        "    acctbal double,\n" +
                        "    mktsegment varchar(10),\n" +
                        "    comment varchar(117), \n" +
                        "    ds varchar(20)\n" +
                        " ) WITH (" +
                        "    format = 'ORC',\n" +
                        "    partitioned_by = array['ds'], \n" +
                        "    clustered_by = array['custkey'], \n" +
                        "    cluster_count = array[10], \n" +
                        "    distribution = array['150', '300', '450', '600', '750', '900', '1050', '1200', '1350'])");
        MaterializedResult result = computeActual(query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), true);

        query = (
                "Insert INTO hive_bucketed.tpch_bucketed.customer_created \n" +
                "SELECT *, '2021-05-14' from hive_bucketed.tpch_bucketed.customer LIMIT 10");
        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 10L);

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_created";
        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 10L);

        query = (
                "SELECT custkey, COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_created \n" +
                "GROUP BY custkey ORDER BY custkey LIMIT 1 ");
        result = computeActual(query);
        assertEquals(result.getRowCount(), 1L);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 1L);
    }

    @Test
    public void testMultipleClusterColumns()
    {
        String query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_multiple \n" +
                        " WITH (" +
                        "    format = 'ORC',\n" +
                        "    clustered_by = array['custkey', 'acctbal'], \n" +
                        "    cluster_count = array[8, 8], \n" +
                        "    distribution = array['150', '300', '450', '600', '750', '900', '1050',\n" +
                        "    '81.06', '1317.56', '2292.67', '3274.3', '4344.52', '5527.61', '6557.51']) \n" +
                        " AS SELECT * FROM customer");

        MaterializedResult result = computeActual(query);
        assertEquals(result.getOnlyValue(), 1500L);

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple WHERE custkey <= 100";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        QueryInfo queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                22.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                22.0);

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple WHERE acctbal <= 1000.0";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                43.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                43.0);

        query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_multiple_again \n" +
                        " WITH (" +
                        "    format = 'ORC',\n" +
                        "    bucketed_by = array['custkey', 'acctbal'], \n" +
                        "    bucket_count = 64)\n" +
                        " AS SELECT * FROM customer");

        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 1500L);

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple_again WHERE custkey <= 100";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                153.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                153.0);

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple_again WHERE acctbal <= 1000.0";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                183.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                183.0);
    }
}
