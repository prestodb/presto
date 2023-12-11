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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class SelectResult
{
    private final Map<String, Object> results;

    public SelectResult(Map<String, Object> checksums)
    {
        this.results = unmodifiableMap(requireNonNull(checksums, "results is null"));
    }

    public static Optional<SelectResult> fromResultSet(ResultSet resultSet)
            throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();
        Map<String, Object> results = new HashMap<>(metaData.getColumnCount());

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            Object data = resultSet.getObject(i);
            results.put(columnName, data);
        }
        return Optional.of(new SelectResult(results));
    }

    public Object getData(String columnName)
    {
        checkArgument(results.containsKey(columnName), "column does not exist: %s", columnName);
        return results.get(columnName);
    }
}
