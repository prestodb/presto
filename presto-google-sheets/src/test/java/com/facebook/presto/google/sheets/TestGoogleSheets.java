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
package com.facebook.presto.google.sheets;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static com.facebook.presto.google.sheets.TestSheetsPlugin.getTestCredentialsPath;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestGoogleSheets
        extends AbstractTestQueryFramework
{
    protected static final String GOOGLE_SHEETS = "gsheets";
    protected static final String SHEET_RANGE = "Sheet1";
    protected static final String SHEET_VALUE_INPUT_OPTION = "RAW";
    protected static final String DRIVE_PERMISSION_TYPE = "user";
    protected static final String DRIVE_PERMISSION_ROLE = "writer";

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GOOGLE_SHEETS)
                .setSchema("default")
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner;
        try {
            SheetsPlugin sheetsPlugin = new SheetsPlugin();
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            queryRunner.installPlugin(sheetsPlugin);
            queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, new ImmutableMap.Builder<String, String>()
                    .put("credentials-path", getTestCredentialsPath())
                    .put("metadata-sheet-id", TEST_METADATA_SHEET_ID)
                    .put("sheets-data-max-cache-size", "2000")
                    .put("sheets-data-expire-after-write", "10m")
                    .put("sheets-range", "Sheet1")
                    .put("sheets-value-input-option", "RAW")
                    .put("drive-permission-type", "user")
                    .put("drive-permission-role", "writer")
                    .build());
        }
        catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
        return queryRunner;
    }

    @Test(enabled = false)
    public void testDescTable()
    {
        assertQuery("desc number_text", "SELECT * FROM (VALUES('number','integer','',''), ('text','varchar','',''))");
        assertQuery("desc metadata_table", "SELECT * FROM (VALUES('table name','varchar','',''), ('sheetid_sheetname','varchar','',''), "
                + "('owner','varchar','',''), ('isarchived','varchar','',''),('column_types','varchar','',''))");
    }

    @Test(enabled = false)
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM number_text", "SELECT 5");
        assertQuery("SELECT number FROM number_text", "SELECT * FROM (VALUES '1','2','3','4','5')");
        assertQuery("SELECT text FROM number_text", "SELECT * FROM (VALUES 'one','two','three','four','five')");
        assertQuery("SELECT * FROM number_text", "SELECT * FROM (VALUES ('1','one'), ('2','two'), ('3','three'), ('4','four'), ('5','five'))");
    }

    @Test(enabled = false)
    public void testSelectFromTableIgnoreCase()
    {
        assertQuery("SELECT count(*) FROM NUMBER_TEXT", "SELECT 5");
        assertQuery("SELECT number FROM Number_Text", "SELECT * FROM (VALUES '1','2','3','4','5')");
    }

    @Test(enabled = false)
    public void testQueryingUnknownSchemaAndTable()
    {
        assertQueryFails("select * from gsheets.foo.bar", "line 1:15: Schema foo does not exist");
        assertQueryFails("select * from gsheets.default.foo_bar_table", "line 1:15: Table gsheets.default.foo_bar_table does not exist");
    }

    @Test(enabled = false)
    public void testTableWithRepeatedAndMissingColumnNames()
    {
        assertQuery("desc table_with_duplicate_and_missing_column_names", "SELECT * FROM (VALUES('a','varchar','','')," +
                " ('column_1','varchar','',''), ('column_2','varchar','',''), ('c','varchar','',''))");
    }
}
