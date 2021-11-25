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
package com.facebook.presto.plugin.clickhouse;

import static com.facebook.presto.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static com.facebook.presto.tests.datatype.DataType.bigintDataType;
import static com.facebook.presto.tests.datatype.DataType.doubleDataType;
import static com.facebook.presto.tests.datatype.DataType.integerDataType;
import static com.facebook.presto.tests.datatype.DataType.realDataType;
import static com.facebook.presto.tests.datatype.DataType.smallintDataType;
import static com.facebook.presto.tests.datatype.DataType.tinyintDataType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.datatype.CreateAndInsertDataSetup;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataSetup;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

public class TestClickHouseTypeMapping
        extends AbstractTestQueryFramework
{
    private TestingClickHouseServer clickhouseServer;

    private static void checkIsGap(ZoneId zone, LocalDateTime dateTime)
    {
        verify(isGap(zone, dateTime), "Expected %s to be a gap in %s", dateTime, zone);
    }

    private static boolean isGap(ZoneId zone, LocalDateTime dateTime)
    {
        return zone.getRules().getValidOffsets(dateTime).isEmpty();
    }

    private static void checkIsDoubled(ZoneId zone, LocalDateTime dateTime)
    {
        verify(zone.getRules().getValidOffsets(dateTime).size() == 2, "Expected %s to be doubled in %s", dateTime, zone);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = new TestingClickHouseServer();
        return createClickHouseQueryRunner(clickhouseServer,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        clickhouseServer.close();
    }

    @Test
    public void testDouble()
    {
        // TODO SqlDataTypeTest
        DataTypeTest.create()
                .addRoundTrip(doubleDataType(), 3.1415926835)
                .addRoundTrip(doubleDataType(), 1.79769E+308)
                .addRoundTrip(doubleDataType(), 2.225E-307)

//                .execute(getQueryRunner(), prestoCreateAndInsert("tpch.test_decimal"))

                // TODO test ClickHouse Nullable(...)
                .addRoundTrip(doubleDataType(), null)

                .execute(getQueryRunner(), prestoCreateAsSelect("presto_test_double"));
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return prestoCreateAsSelect(getSession(), tableNamePrefix);
    }

    private DataSetup prestoCreateAsSelect(Session session, String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new ClickHouseSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }

    private DataSetup prestoCreateAndInsert(String tableNamePrefix)
    {
        return prestoCreateAndInsert(getSession(), tableNamePrefix);
    }

    private DataSetup prestoCreateAndInsert(Session session, String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(new ClickHouseSqlExecutor(getQueryRunner(), session), tableNamePrefix);
    }
}