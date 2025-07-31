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
package com.facebook.presto.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SourceColumn
{
    private final QualifiedObjectName tableName;
    private final String columnName;

    @JsonCreator
    public SourceColumn(
            @JsonProperty("tableName") QualifiedObjectName tableName,
            @JsonProperty("columnName") String columnName)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    @JsonProperty
    public QualifiedObjectName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, columnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SourceColumn entry = (SourceColumn) obj;
        return Objects.equals(tableName, entry.tableName) &&
                Objects.equals(columnName, entry.columnName);
    }

    @Override
    public String toString()
    {
        return "SourceColumn{" +
                "tableName=" + tableName +
                ", columnName='" + columnName + '\'' +
                '}';
    }
}
