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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

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

        query = "DESCRIBE hive_bucketed.tpch_bucketed.customer_copied_again";
        result = computeActual(query);
        assertEquals(result.getMaterializedRows().size(), 8);

        query = "SHOW CREATE TABLE hive_bucketed.tpch_bucketed.customer_copied_again";
        result = computeActual(query);
        assertEquals(result.getMaterializedRows().size(), 8);
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

        query = (
                "Insert INTO hive_bucketed.tpch_bucketed.customer_created (custkey, ds) \n" +
                        "VALUES (NULL, '2021-05-15'), (2000, '2021-05-15'), (1, '2021-05-15')");
        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 3L);

        query = (
                "SELECT \"$path\", COUNT(1) FROM hive_bucketed.tpch_bucketed.customer_created WHERE custkey = 1 GROUP BY 1 ORDER BY 1");
        result = computeActual(query);
        assertEquals(result.getRowCount(), 1L);
        String pathForFirstCluster = (String) result.getMaterializedRows().get(0).getField(0);

        query = (
                "SELECT \"$path\", COUNT(1) FROM hive_bucketed.tpch_bucketed.customer_created WHERE custkey IS NULL GROUP BY 1 ORDER BY 1");
        result = computeActual(query);
        assertEquals(result.getRowCount(), 1L);
        String pathForNullValue = (String) result.getMaterializedRows().get(0).getField(0);
        assertEquals(pathForNullValue, pathForFirstCluster);
    }

    @Test
    public void testMultipleClusterColumns()
    {
        String query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_multiple \n" +
                        " WITH (" +
                        "    format = 'ORC',\n" +
                        "    partitioned_by = array['ds'], \n" +
                        "    clustered_by = array['custkey', 'acctbal'], \n" +
                        "    cluster_count = array[8, 8], \n" +
                        "    distribution = array['150', '300', '450', '600', '750', '900', '1050',\n" +
                        "    '81.06', '1317.56', '2292.67', '3274.3', '4344.52', '5527.61', '6557.51']) \n" +
                        " AS SELECT *, '2021-05-01' AS ds FROM customer");

        MaterializedResult result = computeActual(query);
        assertEquals(result.getOnlyValue(), 1500L);

        query = "SELECT \"$path\", COUNT(1) FROM hive_bucketed.tpch_bucketed.customer_multiple GROUP BY 1 ORDER BY 1";
        result = computeActual(query);
        long fileNumWithTwoDimensions = result.getRowCount();
        long rowCount = 0;
        for (int i = 0; i < result.getRowCount(); ++i) {
            assertTrue((long) result.getMaterializedRows().get(i).getField(1) > 0);
            rowCount += (long) result.getMaterializedRows().get(i).getField(1);
        }
        assertEquals(rowCount, 1500L);
        assertEquals(result.toString(), "");

        List<Integer> custkeySamples = new ArrayList<>(Arrays.asList(150, 300, 450, 600, 750, 900, 1050));
        double[] acctbalPoints = {81.06, 1317.56, 2292.67, 3274.3, 4344.52, 5527.61, 6557.51};
        List<Double> acctbalSamples = DoubleStream.of(acctbalPoints).boxed().collect(Collectors.toList());
        assertEquals(custkeySamples.size(), acctbalSamples.size());
        for (int i = 1; i < custkeySamples.size(); ++i) {
            query = format(
                    "SELECT \"$path\", COUNT(1) FROM hive_bucketed.tpch_bucketed.customer_multiple " +
                            "WHERE custkey > %d and custkey <= %d and acctbal > %f and acctbal <= %f  GROUP BY 1 ORDER BY 1",
                    custkeySamples.get(i - 1),
                    custkeySamples.get(i),
                    acctbalSamples.get(i - 1),
                    acctbalSamples.get(i));
            result = computeActual(query);
            assertTrue(result.getRowCount() <= 1);
        }

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple WHERE custkey <= 100";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        QueryInfo queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                24.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                24.0);

        query = "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple WHERE acctbal <= 1000.0";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                47.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                47.0);

        query = (
                "SELECT COUNT(*) FROM hive_bucketed.tpch_bucketed.customer_multiple " +
                        "WHERE acctbal <= 80.0 and custkey = 200");
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), query);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getProcessedInputDataSize().getValue()),
                11.0);
        assertEquals(
                Math.ceil(queryInfo.getQueryStats().getRawInputDataSize().getValue()),
                11.0);

        query = (
                "INSERT INTO hive_bucketed.tpch_bucketed.customer_multiple (custkey, acctbal, ds) VALUES " +
                        "(NULL, 1.0, '2021-05-02'), " +  // first file
                        "(1, NULL, '2021-05-02'), " +    // first file
                        "(1, 1.0, '2021-05-02'), " +     // first file
                        "(NULL, NULL, '2021-05-02'), " +   // first file
                        "(NULL, 100.0, '2021-05-02'), " +  // second file
                        "(1, 100.0, '2021-05-02'), " +     // second file
                        "(200, NULL, '2021-05-02'), " +    // third file
                        "(200, 100.0, '2021-05-02')");   // fourth file
        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 8L);

        query = (
                "SELECT \"$path\", COUNT(1) FROM hive_bucketed.tpch_bucketed.customer_multiple " +
                        "WHERE ds = '2021-05-02' GROUP BY 1 order by 1");
        result = computeActual(query);
        assertEquals(result.getRowCount(), 4);

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

        query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_with_three_dimensions \n" +
                        " WITH (" +
                        "    format = 'ORC',\n" +
                        "    partitioned_by = array['ds'], \n" +
                        "    clustered_by = array['custkey', 'acctbal', 'mktsegment'], \n" +
                        "    cluster_count = array[8, 8, 2], \n" +
                        "    distribution = array['150', '300', '450', '600', '750', '900', '1050',\n" +
                        "    '81.06', '1317.56', '2292.67', '3274.3', '4344.52', '5527.61', '6557.51', 'H']) \n" +
                        " AS SELECT *, '2021-05-01' AS ds FROM customer");
        result = computeActual(query);
        assertEquals(result.getOnlyValue(), 1500L);

        query = "SELECT \"$path\", COUNT(1) FROM hive_bucketed.tpch_bucketed.customer_with_three_dimensions GROUP BY 1 ORDER BY 1";
        result = computeActual(query);
        long fileNumWithThreeDimensions = result.getRowCount();
        rowCount = 0;
        for (int i = 0; i < result.getRowCount(); ++i) {
            assertTrue((long) result.getMaterializedRows().get(i).getField(1) > 0);
            rowCount += (long) result.getMaterializedRows().get(i).getField(1);
        }
        assertEquals(rowCount, 1500L);
        assertEquals(fileNumWithThreeDimensions, fileNumWithTwoDimensions * 2);
    }
}
