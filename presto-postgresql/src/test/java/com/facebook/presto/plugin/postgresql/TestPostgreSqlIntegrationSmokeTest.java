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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.facebook.presto.tests.datatype.CreateAndInsertDataSetup;
import com.facebook.presto.tests.datatype.CreateAsSelectDataSetup;
import com.facebook.presto.tests.datatype.DataSetup;
import com.facebook.presto.tests.datatype.DataType;
import com.facebook.presto.tests.datatype.DataTypeTest;
import com.facebook.presto.tests.sql.JdbcSqlExecutor;
import com.facebook.presto.tests.sql.PrestoSqlExecutor;
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.function.Function;

import static com.facebook.presto.tests.datatype.DataType.varcharDataType;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestPostgreSqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPostgreSqlServer postgreSqlServer;

    public TestPostgreSqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlIntegrationSmokeTest(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(() -> PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ORDERS));
        this.postgreSqlServer = postgreSqlServer;
        execute("CREATE EXTENSION file_fdw");
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        postgreSqlServer.close();
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
    }

    @Test
    public void testForeignTable()
            throws Exception
    {
        execute("CREATE SERVER devnull FOREIGN DATA WRAPPER file_fdw");
        execute("CREATE FOREIGN TABLE tpch.test_ft (x bigint) SERVER devnull OPTIONS (filename '/dev/null')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_ft"));
        computeActual("SELECT * FROM test_ft");
        execute("DROP FOREIGN TABLE tpch.test_ft");
        execute("DROP SERVER devnull");
    }

    @Test
    public void testPrestoCreatedParameterizedVarchar()
    {
        varcharDataTypeTest().execute(getQueryRunner(), prestoCreateAsSelect("presto_test_parameterized_varchar"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarchar()
    {
        varcharDataTypeTest().execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_varchar"));
    }

    private static DataTypeTest varcharDataTypeTest()
    {
        return DataTypeTest.create()
                .addRoundTrip(varcharDataType(10), "text_a")
                .addRoundTrip(varcharDataType(255), "text_b")
                .addRoundTrip(varcharDataType(65535), "text_d")
                .addRoundTrip(varcharDataType(10485760), "text_f")
                .addRoundTrip(varcharDataType(), "unbounded");
    }

    @Test
    public void testPrestoCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest().execute(getQueryRunner(), prestoCreateAsSelect("postgresql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedVarcharUnicode()
    {
        unicodeVarcharDateTypeTest().execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_varchar_unicode"));
    }

    @Test
    public void testPrestoCreatedParameterizedCharUnicode()
    {
        unicodeDataTypeTest(DataType::charDataType).execute(getQueryRunner(), prestoCreateAsSelect("postgresql_test_parameterized_char_unicode"));
    }

    @Test
    public void testPostgreSqlCreatedParameterizedCharUnicode()
    {
        unicodeDataTypeTest(DataType::charDataType).execute(getQueryRunner(), postgresCreateAndInsert("tpch.postgresql_test_parameterized_char_unicode"));
    }

    private static DataTypeTest unicodeVarcharDateTypeTest()
    {
        return unicodeDataTypeTest(DataType::varcharDataType)
                .addRoundTrip(varcharDataType(), "\u041d\u0443, \u043f\u043e\u0433\u043e\u0434\u0438!");
    }

    private static DataTypeTest unicodeDataTypeTest(Function<Integer, DataType<String>> dataTypeFactory)
    {
        String sampleUnicodeText = "\u653b\u6bbb\u6a5f\u52d5\u968a";
        String sampleFourByteUnicodeCharacter = "\uD83D\uDE02";

        return DataTypeTest.create()
                .addRoundTrip(dataTypeFactory.apply(sampleUnicodeText.length()), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(32), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(20000), sampleUnicodeText)
                .addRoundTrip(dataTypeFactory.apply(1), sampleFourByteUnicodeCharacter);
    }

    private DataSetup prestoCreateAsSelect(String tableNamePrefix)
    {
        return new CreateAsSelectDataSetup(new PrestoSqlExecutor(getQueryRunner()), tableNamePrefix);
    }

    private DataSetup postgresCreateAndInsert(String tableNamePrefix)
    {
        return new CreateAndInsertDataSetup(postgresExecutor(), tableNamePrefix);
    }

    private void execute(String sql)
    {
        postgresExecutor().execute(sql);
    }

    private JdbcSqlExecutor postgresExecutor()
    {
        return new JdbcSqlExecutor(postgreSqlServer.getJdbcUrl());
    }
}
