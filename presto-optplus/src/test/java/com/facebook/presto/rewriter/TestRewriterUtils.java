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
package com.facebook.presto.rewriter;

import com.facebook.presto.client.Column;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.rewriter.optplus.RewriterUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.testng.Assert.assertEquals;

public class TestRewriterUtils
{
    @Test
    public void testGeneratedValuesQuery()
    {
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            List<Object> tempRow = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                tempRow.add(j);
            }
            rows.add(tempRow);
        }
        String expected = " SELECT * FROM (VALUES (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2), (0, 1, 2))" +
                " AS t (\"col1\", \"col2\", \"col3\")";
        List<Column> columns = unmodifiableList(asList(new Column("col1", SmallintType.SMALLINT),
                new Column("col2", IntegerType.INTEGER),
                new Column("col3", TinyintType.TINYINT)));
        assertEquals(RewriterUtils.generateValuesQuery(rows, columns), expected);
    }

    @Test
    public void testGeneratedValuesQueryWithNonIntTypes()
    {
        List<List<Object>> rows = new ArrayList<>();
        List<Column> columns = unmodifiableList(asList(new Column("col1", TimestampType.TIMESTAMP),
                new Column("col2", UuidType.UUID),
                new Column("col3", DateType.DATE),
                new Column("col4", VarcharType.VARCHAR),
                // Column name as digit is added on purpose as DB2 produces such outputs.
                new Column("5", VarcharType.VARCHAR)));
        List<Object> tempRow = asList("2001-08-22 03:04:05.321", "12151fd2-7586-11e9-8f9e-2a86e4085a59", "2001-08-22", "bye", "1");
        rows.add(tempRow);

        String expected = " SELECT * FROM (VALUES (TIMESTAMP '2001-08-22 03:04:05.321', UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59'," +
                " DATE '2001-08-22', 'bye', '1')) AS t (\"col1\", \"col2\", \"col3\", \"col4\", \"5\")";
        assertEquals(RewriterUtils.generateValuesQuery(rows, columns), expected);
    }

    @Test
    public void testGeneratedValuesQueryWithEmptyRows()
    {
        String expected = "SELECT CHAR '<>'";
        assertEquals(RewriterUtils.generateValuesQuery(emptyList(), emptyList()), expected);
    }

    @Test
    public void testGeneratedValuesQueryWithEmptyColumnsInformation()
    {
        List<List<Object>> rows = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            List<Object> tempRow = new ArrayList<>();
            for (int j = 0; j < 4; j++) {
                tempRow.add(j);
            }
            rows.add(tempRow);
        }

        String expected = " SELECT * FROM (VALUES ('0', '1', '2', '3'))";
        assertEquals(RewriterUtils.generateValuesQuery(rows, emptyList()), expected);
    }

    @Test
    public void testGeneratedValuesQueryWithMismatchingColumnSize()
    {
        List<List<Object>> rows = new ArrayList<>();
        List<Column> columns = unmodifiableList(asList(new Column("col1", IntegerType.INTEGER),
                new Column("col2", IntegerType.INTEGER),
                new Column("col3", IntegerType.INTEGER)));
        for (int i = 0; i < 1; i++) {
            List<Object> tempRow = new ArrayList<>();
            for (int j = 0; j < 4; j++) {
                tempRow.add(j);
            }
            rows.add(tempRow);
        }
        // When column information mismatches i.e. result has more columns than ColumnNames list provided
        // we assume as if no column info available.
        String expected = " SELECT * FROM (VALUES ('0', '1', '2', '3'))";
        assertEquals(RewriterUtils.generateValuesQuery(rows, columns), expected);
    }
}
