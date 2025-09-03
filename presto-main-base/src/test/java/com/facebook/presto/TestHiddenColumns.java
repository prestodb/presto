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

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestHiddenColumns
{
    private LocalQueryRunner runner;

    @BeforeClass
    public void setUp()
    {
        runner = new LocalQueryRunner(TEST_SESSION);
        runner.createCatalog(TEST_SESSION.getCatalog().get(), new TpchConnectorFactory(1), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (runner != null) {
            runner.close();
            runner = null;
        }
    }

    @Test
    public void testDescribeTable()
    {
        MaterializedResult expected = MaterializedResult.resultBuilder(TEST_SESSION, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BIGINT)
                .row("regionkey", "bigint", "", "", 19L, null, null)
                .row("name", "varchar(25)", "", "", null, null, 25L)
                .row("comment", "varchar(152)", "", "", null, null, 152L)
                .build();
        assertEquals(runner.execute("DESC REGION"), expected);
    }

    @Test
    public void testSimpleSelect()
    {
        assertEquals(runner.execute("SELECT * from REGION"), runner.execute("SELECT regionkey, name, comment from REGION"));
        assertEquals(runner.execute("SELECT *, row_number from REGION"), runner.execute("SELECT regionkey, name, comment, row_number from REGION"));
        assertEquals(runner.execute("SELECT row_number, * from REGION"), runner.execute("SELECT row_number, regionkey, name, comment from REGION"));
        assertEquals(runner.execute("SELECT *, row_number, * from REGION"), runner.execute("SELECT regionkey, name, comment, row_number, regionkey, name, comment from REGION"));
        assertEquals(runner.execute("SELECT row_number, x.row_number from REGION x"), runner.execute("SELECT row_number, row_number from REGION"));
    }

    @Test
    public void testAliasedTableColumns()
    {
        // https://github.com/prestodb/presto/issues/11385
        // TPCH tables have a hidden "row_number" column, which triggers this bug.
        assertEquals(
                runner.execute("SELECT * FROM orders AS t (a, b, c, d, e, f, g, h, i)"),
                runner.execute("SELECT * FROM orders"));
    }
}
