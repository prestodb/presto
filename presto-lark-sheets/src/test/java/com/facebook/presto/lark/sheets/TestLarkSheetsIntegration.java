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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.lark.sheets.TestLarkSheetsPlugin.getTestingConnectorConfig;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestLarkSheetsIntegration
        extends AbstractTestQueryFramework
{
    // Used for fast switch to enable or disable tests
    private static final boolean TEST_ENABLED = false;
    private static final String CATALOG = "larksheets";
    // The spreadsheet for this integration test is at
    // https://test-ch80md45anra.feishu.cn/sheets/shtcnBf5pg4BNSkwV2Ku5xwW9Pf
    private static final String TESTING_TOKEN = "shtcnBf5pg4BNSkwV2Ku5xwW9Pf";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LarkSheetsQueryRunners.createSheetsQueryRunner(CATALOG, ImmutableMap.of(), getTestingConnectorConfig());
    }

    @Test(enabled = TEST_ENABLED)
    public void testSchemaManipulations()
    {
        final String schema = "test_schema_manipulations_0";
        Session session = testSessionBuilder().setCatalog(CATALOG).build();
        Identity anotherIdentity = new Identity("user_another", Optional.empty());
        Session anotherSession = testSessionBuilder().setIdentity(anotherIdentity).setCatalog(CATALOG).build();

        // create schema
        assertUpdate(session, format("CREATE SCHEMA %s WITH (TOKEN = '%s')", schema, TESTING_TOKEN));

        // create duplicate schema by others
        assertQueryFails(anotherSession,
                format("CREATE SCHEMA %s WITH (TOKEN = '%s')", schema, TESTING_TOKEN),
                "Schema '.*' already exists or created by others");

        // visible to me
        assertQuery(session,
                "SHOW SCHEMAS LIKE 'test_schema_manipulations_%'",
                format("SELECT * FROM (VALUES '%s')", schema));

        // invisible to others
        assertQuery(anotherSession,
                "SHOW SCHEMAS LIKE 'test_schema_manipulations_%'",
                "SELECT * FROM (VALUES '') LIMIT 0");

        // show tables
        assertQuery(session,
                format("SHOW TABLES FROM %s", schema),
                "SELECT * FROM (VALUES 'number_text', 'missing_columns', 'duplicate_columns')");

        // show tables by others
        assertQueryFails(anotherSession,
                format("SHOW TABLES FROM %s", schema),
                "line .*: Schema '.*' does not exist");

        // drop schema by others
        assertQueryFails(anotherSession,
                format("DROP SCHEMA %s", schema),
                "line .*: Schema '.*' does not exist");

        // drop schema by me
        assertUpdate(session, format("DROP SCHEMA %s", schema));
        assertQuery(session,
                "SHOW SCHEMAS LIKE 'test_schema_manipulations_%'",
                "SELECT * FROM (VALUES '') LIMIT 0");
    }

    @Test(enabled = TEST_ENABLED)
    public void testPublicSchema()
    {
        final String schema = "test_public_schema_0";
        final String anotherUser = "user_another";
        final Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(schema).build();
        final Session anotherSession = testSessionBuilder()
                .setIdentity(new Identity(anotherUser, Optional.empty()))
                .setCatalog(CATALOG)
                .setSchema(schema)
                .build();

        // create schema
        assertUpdate(session, format("CREATE SCHEMA %s WITH (TOKEN = '%s', PUBLIC = true)", schema, TESTING_TOKEN));

        // show tables by me
        assertQuery(session,
                format("SHOW TABLES FROM %s", schema),
                "SELECT * FROM (VALUES 'number_text', 'missing_columns', 'duplicate_columns')");

        // show tables by others
        assertQuery(anotherSession,
                format("SHOW TABLES FROM %s", schema),
                "SELECT * FROM (VALUES 'number_text', 'missing_columns', 'duplicate_columns')");

        // desc table by me
        assertQuery(session,
                "DESC number_text",
                "SELECT * FROM (VALUES ('number', 'varchar', '', ''), ('text', 'varchar', '', ''))");

        // desc table by others
        assertQuery(anotherSession,
                "DESC number_text",
                "SELECT * FROM (VALUES ('number', 'varchar', '', ''), ('text', 'varchar', '', ''))");

        // select by me
        assertQuery(session,
                "SELECT number FROM number_text",
                "SELECT * FROM (VALUES '1.0', '2.0', '3.0', '4.0', '5.0')");

        // select by others
        assertQuery(anotherSession,
                "SELECT number FROM number_text",
                "SELECT * FROM (VALUES '1.0', '2.0', '3.0', '4.0', '5.0')");

        // drop schema by others
        assertQueryFails(anotherSession,
                format("DROP SCHEMA %s", schema),
                format("User '%s' is not permitted to perform 'drop' on schema '%s'", anotherUser, schema));

        // drop schema by me
        assertUpdate(session, format("DROP SCHEMA %s", schema));
    }

    @Test(enabled = TEST_ENABLED)
    public void testMetadataAndSystemTable()
    {
        String schema = "test_metadata_and_system_table_0";
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(schema).build();

        assertUpdate(session, format("CREATE SCHEMA %s WITH (TOKEN = '%s')", schema, TESTING_TOKEN));

        assertQuery(session,
                "SELECT * FROM \"$sheets\"",
                "SELECT * FROM (VALUES (0, 'MT1p4I', 'number_text'), (1, 'flbtGk', 'missing_columns'), (2, 'f6Jbrw', 'duplicate_columns'))");

        assertQuery(session,
                "DESC number_text",
                "SELECT * FROM (VALUES ('number', 'varchar', '', ''), ('text', 'varchar', '', ''))");

        assertQuery(session,
                "DESC \"$0\"",
                "SELECT * FROM (VALUES ('number', 'varchar', '', ''), ('text', 'varchar', '', ''))");

        assertQuery(session,
                "DESC \"@MT1p4I\"",
                "SELECT * FROM (VALUES ('number', 'varchar', '', ''), ('text', 'varchar', '', ''))");
    }

    @Test(enabled = TEST_ENABLED)
    public void testSelectTable()
    {
        String schema = "test_select_table_0";
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(schema).build();
        assertUpdate(session, format("CREATE SCHEMA %s WITH (TOKEN = '%s')", schema, TESTING_TOKEN));

        // select all columns
        assertQuery(session,
                "SELECT * FROM number_text",
                "SELECT * FROM (VALUES ('1.0', 'one'), ('2.0', 'two'), ('3.0', 'three'), ('4.0', 'four'), ('5.0', 'five'))");

        // select by sheet index
        assertQuery(session,
                "SELECT * FROM \"$0\"",
                "SELECT * FROM (VALUES ('1.0', 'one'), ('2.0', 'two'), ('3.0', 'three'), ('4.0', 'four'), ('5.0', 'five'))");

        // select by sheet id
        assertQuery(session,
                "SELECT * FROM \"@MT1p4I\"",
                "SELECT * FROM (VALUES ('1.0', 'one'), ('2.0', 'two'), ('3.0', 'three'), ('4.0', 'four'), ('5.0', 'five'))");

        // select named columns
        assertQuery(session,
                "SELECT number, text FROM number_text",
                "SELECT * FROM (VALUES ('1.0', 'one'), ('2.0', 'two'), ('3.0', 'three'), ('4.0', 'four'), ('5.0', 'five'))");

        // select named columns (reordered)
        assertQuery(session,
                "SELECT text, number FROM number_text",
                "SELECT * FROM (VALUES ('one', '1.0'), ('two', '2.0'), ('three', '3.0'), ('four', '4.0'), ('five', '5.0'))");

        // select projected columns
        assertQuery(session,
                "SELECT number FROM number_text",
                "SELECT * FROM (VALUES '1.0', '2.0', '3.0', '4.0', '5.0')");

        // select projected columns
        assertQuery(session,
                "SELECT text FROM number_text",
                "SELECT * FROM (VALUES 'one', 'two', 'three', 'four', 'five')");
    }

    @Test(enabled = TEST_ENABLED)
    public void testSpecialTable()
    {
        String schema = "test_special_table_0";
        Session session = testSessionBuilder().setCatalog(CATALOG).setSchema(schema).build();
        assertUpdate(session, format("CREATE SCHEMA %s WITH (TOKEN = '%s')", schema, TESTING_TOKEN));

        // normal table
        assertQuery(session,
                "DESC number_text",
                "SELECT * FROM (VALUES ('number', 'varchar', '', ''), ('text', 'varchar', '', ''))");
        assertQuery(session,
                "SELECT * FROM number_text",
                "SELECT * FROM (VALUES ('1.0', 'one'), ('2.0', 'two'), ('3.0', 'three'), ('4.0', 'four'), ('5.0', 'five'))");

        // table with missing columns
        assertQuery(session,
                "DESC missing_columns",
                "SELECT * FROM (VALUES ('id', 'varchar', '', ''), ('name', 'varchar', '', ''), ('tag', 'varchar', '', ''))");
        assertQuery(session,
                "SELECT * FROM missing_columns",
                "SELECT * FROM (VALUES ('1.0', 'one', 'A'), ('2.0', 'two', 'B'))");

        // table with duplicate columns
        assertQueryFails(session,
                "DESC duplicate_columns",
                "Duplicated name id in Column#.* and Column#.*");
        assertQueryFails(session,
                "SELECT * FROM duplicate_columns",
                "Duplicated name id in Column#.* and Column#.*");
    }
}
