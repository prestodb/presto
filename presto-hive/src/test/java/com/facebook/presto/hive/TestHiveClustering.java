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

import com.facebook.presto.hive.TestHiveEventListenerPlugin.TestingHiveEventListener;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveClustering
        extends AbstractTestDistributedQueries
{
    public TestHiveClustering()
    {
        super(() -> createQueryRunner(getTables()));
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
    public void testCreateTable()
    {
        String query = (
                "CREATE TABLE hive_bucketed.tpch_bucketed.customer_test (\n" +
                        "    custkey bigint,\n" +
                        "    name varchar(25),\n" +
                        "    address varchar(40),\n" +
                        "    nationkey bigint,\n" +
                        "    phone varchar(15),\n" +
                        "    acctbal double,\n" +
                        "    mktsegment varchar(10),\n" +
                        "    comment varchar(117)\n" +
                        " ) WITH (" +
                        "    format = 'TEXTFILE',\n" +
                        "    clustered_by = array['custkey'], \n" +
                        "    cluster_count = array[11], \n" +
                        "    distribution = array['150', '300', '450', '600', '750', '900', '1050', '1200', '1350'])");

        MaterializedResult result = computeActual(query);
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), true);
    }
}
