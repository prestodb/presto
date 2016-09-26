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
package com.facebook.presto.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MaterializedQueryTableInfo
{
    public static final long DEFAULT_REFRESH_TIMESTAMP = -1;

    private final String query;
    private final Map<String, byte[]> tableIdentities;
    private final Map<String, Map<String, byte[]>> columnIdentities;
    private final long lastRefreshTimestamp;

    @JsonCreator
    public MaterializedQueryTableInfo(
            @JsonProperty("query") String query,
            @JsonProperty("tableIdentities") Map<String, byte[]> tableIdentities,
            @JsonProperty("columnIdentities") Map<String, Map<String, byte[]>> columnIdentities,
            @JsonProperty("lastRefreshTimestamp") long lastRefreshTimestamp)
    {
        this.query = requireNonNull(query, "query is null");
        this.tableIdentities = requireNonNull(tableIdentities, "tableIdentities is null");
        this.columnIdentities = requireNonNull(columnIdentities, "columnIdentities is null");
        this.lastRefreshTimestamp = lastRefreshTimestamp;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Map<String, byte[]> getTableIdentities()
    {
        return tableIdentities;
    }

    @JsonProperty
    public Map<String, Map<String, byte[]>> getColumnIdentities()
    {
        return columnIdentities;
    }

    @JsonProperty
    public long getLastRefreshTimestamp()
    {
        return lastRefreshTimestamp;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaterializedQueryTableInfo that = (MaterializedQueryTableInfo) o;
        return Objects.equals(query, that.query) &&
                Objects.equals(tableIdentities, that.tableIdentities) &&
                Objects.equals(columnIdentities, that.columnIdentities) &&
                Objects.equals(lastRefreshTimestamp, that.lastRefreshTimestamp);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, tableIdentities, columnIdentities);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("MaterializedQueryTableInfo{");
        sb.append("query=").append(query);
        sb.append(", tableIdentities=").append(tableIdentities);
        sb.append(", columnIdentities=").append(columnIdentities);
        sb.append(", lastRefreshTimestamp=").append(lastRefreshTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
