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
package com.facebook.presto.verifier.checksum;

import com.facebook.presto.common.type.SqlVarbinary;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class ChecksumResult
{
    private final long rowCount;
    private final Map<String, Object> checksums;

    public ChecksumResult(long rowCount, Map<String, Object> checksums)
    {
        this.rowCount = rowCount;
        this.checksums = unmodifiableMap(requireNonNull(checksums, "checksums is null"));
        for (Object checksum : checksums.values()) {
            checkArgument(
                    checksum == null || checksum instanceof Long || checksum instanceof Double || checksum instanceof SqlVarbinary,
                    "checksum can only be one of (null, Long, Double, SqlVarbinary), found %s",
                    checksum == null ? "null" : checksum.getClass().getName());
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public Object getChecksum(String columnName)
    {
        checkArgument(checksums.containsKey(columnName), "column does not exist: %s", columnName);
        return checksums.get(columnName);
    }

    public static Optional<ChecksumResult> fromResultSet(ResultSet resultSet)
            throws SQLException
    {
        long rowCount = resultSet.getLong(1);
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, Object> checksums = new HashMap<>(metaData.getColumnCount());

        for (int i = 2; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            Object checksum = resultSet.getObject(i);
            if (checksum == null) {
                checksums.put(columnName, null);
                continue;
            }

            if (metaData.getColumnType(i) == Types.DOUBLE) {
                checkState(checksum instanceof Double, "Expecting double for column %s, found %s", columnName, checksum.getClass());
                checksums.put(columnName, checksum);
            }
            else if (metaData.getColumnType(i) == Types.BIGINT) {
                checkState(checksum instanceof Long, "Expecting bigint for column %s, found %s", columnName, checksum.getClass());
                checksums.put(columnName, checksum);
            }
            else {
                checkState(checksum instanceof byte[], "Expecting binary for column %s, found %s", columnName, checksum.getClass());
                checksums.put(columnName, new SqlVarbinary((byte[]) checksum));
            }
        }
        return Optional.of(new ChecksumResult(rowCount, checksums));
    }
}
