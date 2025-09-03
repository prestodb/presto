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
package com.facebook.presto.plugin.clp.mockdb.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider.ARCHIVES_TABLE_SUFFIX;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ArchivesTableRows
{
    public static final String COLUMN_PAGINATION_ID = "pagination_id";
    public static final String COLUMN_ID = "id";
    public static final String COLUMN_BEGIN_TIMESTAMP = "begin_timestamp";
    public static final String COLUMN_END_TIMESTAMP = "end_timestamp";

    private final List<String> ids;
    private final List<Long> beginTimestamps;
    private final List<Long> endTimestamps;
    private final int numberOfRows;

    public void insertToTable(Connection connection, String tablePrefix, String tableName)
    {
        final String insertSql = format(
                "INSERT INTO `%s` (`%s`, `%s`, `%s`) VALUES (?, ?, ?)",
                format("%s%s%s", tablePrefix, tableName, ARCHIVES_TABLE_SUFFIX),
                COLUMN_ID,
                COLUMN_BEGIN_TIMESTAMP,
                COLUMN_END_TIMESTAMP);
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (int i = 0; i < numberOfRows; ++i) {
                pstmt.setString(1, ids.get(i));
                pstmt.setLong(2, beginTimestamps.get(i));
                pstmt.setLong(3, endTimestamps.get(i));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public ArchivesTableRows(
            List<String> ids,
            List<Long> beginTimestamps,
            List<Long> endTimestamps)
    {
        assertEquals(ids.size(), beginTimestamps.size());
        assertEquals(beginTimestamps.size(), endTimestamps.size());
        this.ids = ids;
        this.beginTimestamps = beginTimestamps;
        this.endTimestamps = endTimestamps;
        this.numberOfRows = ids.size();
    }
}
