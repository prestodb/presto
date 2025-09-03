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

import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_COLUMN_NAME;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_SUFFIX;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class DatasetsTableRows
{
    private final List<String> names;
    private final List<String> archivesStorageDirectories;
    private final int numOfRows;

    public void insertToTable(Connection connection, String tablePrefix)
    {
        final String insertSql = format(
                "INSERT INTO %s (%s, %s) VALUES (?, ?) " +
                        "ON DUPLICATE KEY UPDATE %s = VALUES(%s)",
                format("%s%s", tablePrefix, DATASETS_TABLE_SUFFIX),
                DATASETS_TABLE_COLUMN_NAME,
                DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY,
                DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY,
                DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY);
        try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
            for (int i = 0; i < numOfRows; ++i) {
                pstmt.setString(1, names.get(i));
                pstmt.setString(2, format("%s%s", archivesStorageDirectories.get(i), names.get(i)));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public DatasetsTableRows(
            List<String> names,
            List<String> archivesStorageDirectories)
    {
        assertEquals(names.size(), archivesStorageDirectories.size());
        this.names = names;
        this.archivesStorageDirectories = archivesStorageDirectories;
        this.numOfRows = names.size();
    }
}
