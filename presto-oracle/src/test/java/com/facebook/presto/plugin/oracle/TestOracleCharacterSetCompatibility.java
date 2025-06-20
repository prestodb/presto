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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test to verify Oracle connector compatibility with non-UTF character sets.
 * This test ensures the orai18n.jar dependency enables proper handling of
 * character sets like WE8ISO8859P9.
 */
public class TestOracleCharacterSetCompatibility
        extends AbstractTestQueryFramework
{
    private final OracleServerTester oracleServer;
    private final QueryRunner queryRunner;

    protected TestOracleCharacterSetCompatibility()
            throws Exception
    {
        this.oracleServer = new OracleServerTester();
        this.queryRunner = createOracleQueryRunner(oracleServer);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (oracleServer != null) {
            oracleServer.close();
        }
    }

    @Test
    public void testSpecialCharacterHandling()
    {
        assertUpdate("CREATE TABLE test_charset (id bigint, text varchar(100))");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_charset"));

        try {
            // Insert characters WE8ISO8859P9 specific
            assertUpdate("INSERT INTO test_charset VALUES (1, 'İstanbul')", 1);
            assertUpdate("INSERT INTO test_charset VALUES (2, 'Çağrı')", 1);
            assertUpdate("INSERT INTO test_charset VALUES (3, 'Şehir')", 1);

            // Verify data can be read correctly
            assertQuery("SELECT COUNT(*) FROM test_charset", "VALUES (3)");
            assertQuery("SELECT text FROM test_charset WHERE id = 1", "VALUES ('İstanbul')");
            assertQuery("SELECT COUNT(*) FROM test_charset WHERE text LIKE '%İ%'", "VALUES (1)");
        }
        finally {
            assertUpdate("DROP TABLE test_charset");
            assertFalse(getQueryRunner().tableExists(getSession(), "test_charset"));
        }
    }
}
