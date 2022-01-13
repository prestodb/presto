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

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static org.testng.Assert.assertEquals;

public class TestSqlFormatter
{
    @Test
    public void testSimpleExpression()
    {
        assetQuery("SELECT id\nFROM\n  public.orders\n");
        assetQuery("SELECT id\nFROM\n  \"public\".\"order\"\n");
        assetQuery("SELECT id\nFROM\n  \"public\".\"order\"\"2\"\n");
    }

    private void assetQuery(String query)
    {
        SqlParser parser = new SqlParser();
        Statement statement = parser.createStatement(query, new ParsingOptions());
        String formattedQuery = getFormattedSql(statement, parser, Optional.empty());
        assertEquals(formattedQuery, query);
    }
}
