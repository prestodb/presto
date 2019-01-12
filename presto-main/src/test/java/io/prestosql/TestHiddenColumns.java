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
package io.prestosql;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;

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
        MaterializedResult expected = MaterializedResult.resultBuilder(TEST_SESSION, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("regionkey", "bigint", "", "")
                .row("name", "varchar(25)", "", "")
                .row("comment", "varchar(152)", "", "")
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
