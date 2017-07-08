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
package com.facebook.presto.tests;

import io.airlift.log.Logger;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requires;
import io.prestodb.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.COMMENT_TABLE;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Requires(ImmutableNationTable.class)
public class CommentTableTests
        extends ProductTest
{
    private static final String TABLE_NAME = "comment_test";

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTables()
    {
        try {
            query(format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        }
        catch (Exception e) {
            Logger.get(getClass()).warn(e, "failed to drop table");
        }
    }

    @Test(groups = COMMENT_TABLE)
    public void addCommentTable()
    {
        String createTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'old comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                TABLE_NAME);

        query(createTableSql);
        QueryResult actualResult = query("SHOW CREATE TABLE " + TABLE_NAME);
        assertEquals(actualResult.row(0).get(0), createTableSql);

        String commentedCreateTableSql = format("" +
                        "CREATE TABLE hive.default.%s (\n" +
                        "   c1 bigint\n" +
                        ")\n" +
                        "COMMENT 'new comment'\n" +
                        "WITH (\n" +
                        "   format = 'RCBINARY'\n" +
                        ")",
                TABLE_NAME);

        query(format("COMMENT ON TABLE %s IS 'new comment'", TABLE_NAME));
        actualResult = query(format("SHOW CREATE TABLE %s", TABLE_NAME));
        assertEquals(actualResult.row(0).get(0), commentedCreateTableSql);
    }
}
