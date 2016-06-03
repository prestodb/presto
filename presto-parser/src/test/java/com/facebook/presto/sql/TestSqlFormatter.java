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
package com.facebook.presto.sql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSqlFormatter
{
    @Test(description = "should create a string from Statement")
    public void testFormatSqlSelectClauseOnly()
            throws Exception
    {
        Statement ast = new SqlParser().createStatement("SELECT 1+2");
        String sql = SqlFormatter.formatSql(ast);

        assertEquals(sql, "SELECT (1 + 2)\n\n");
    }

    @Test(description = "should create a string from Statement")
    public void testFormatSqlGroupByCube()
            throws Exception
    {
        Statement ast = new SqlParser().createStatement("SELECT a, b, sum(c) GROUP BY CUBE(a, b)");
        String sql = SqlFormatter.formatSql(ast);

        assertEquals(sql, "SELECT\n  \"a\"\n, \"b\"\n, \"sum\"(\"c\")\n\nGROUP BY CUBE (\"a\", \"b\")\n");
    }
}
