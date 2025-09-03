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

import com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_COLUMN_NAME;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_COLUMN_TYPE;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_SUFFIX;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ColumnMetadataTableRows
{
    private final List<String> names;
    private final List<ClpSchemaTreeNodeType> types;
    private final int numOfRows;

    public void insertToTable(Connection connection, String tablePrefix, String tableName)
    {
        String insertSql = format(
                "INSERT INTO `%s` (`%s`, `%s`) VALUES (?, ?)",
                format("%s%s%s", tablePrefix, tableName, COLUMN_METADATA_TABLE_SUFFIX),
                COLUMN_METADATA_TABLE_COLUMN_NAME,
                COLUMN_METADATA_TABLE_COLUMN_TYPE);
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (int i = 0; i < numOfRows; ++i) {
                pstmt.setString(1, names.get(i));
                pstmt.setByte(2, types.get(i).getType());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public ColumnMetadataTableRows(List<String> names, List<ClpSchemaTreeNodeType> types)
    {
        assertEquals(names.size(), types.size());
        this.names = names;
        this.types = types;
        numOfRows = names.size();
    }
}
