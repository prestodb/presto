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
package com.facebook.presto;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Locale;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestHiddenColumns
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "tpch", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, Locale.ENGLISH, "address", "agent");

    private LocalQueryRunner runner;

    public TestHiddenColumns()
    {
        runner = new LocalQueryRunner(SESSION);
        runner.createCatalog(SESSION.getCatalog(), new TpchConnectorFactory(runner.getNodeManager(), 1), ImmutableMap.<String, String>of());
    }

    @AfterClass
    public void destroy()
    {
        if (runner != null) {
            runner.close();
        }
    }

    @Test
    public void testDescribeTable()
            throws Exception
    {
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, VARCHAR, VARCHAR, BOOLEAN, BOOLEAN)
                .row("regionkey", "bigint", true, false)
                .row("name", "varchar", true, false)
                .row("comment", "varchar", true, false)
                .build();
        assertEquals(runner.execute("DESC REGION"), expected);
    }

    @Test
    public void testSimpleSelect()
            throws Exception
    {
        assertEquals(runner.execute("SELECT * from REGION"), runner.execute("SELECT regionkey, name, comment from REGION"));
        assertEquals(runner.execute("SELECT *, row_number from REGION"), runner.execute("SELECT regionkey, name, comment, row_number from REGION"));
        assertEquals(runner.execute("SELECT row_number, * from REGION"), runner.execute("SELECT row_number, regionkey, name, comment from REGION"));
        assertEquals(runner.execute("SELECT *, row_number, * from REGION"), runner.execute("SELECT regionkey, name, comment, row_number, regionkey, name, comment from REGION"));
        assertEquals(runner.execute("SELECT row_number, x.row_number from REGION x"), runner.execute("SELECT row_number, row_number from REGION"));
    }
}
