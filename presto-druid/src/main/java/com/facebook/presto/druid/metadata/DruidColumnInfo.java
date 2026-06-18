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
package com.facebook.presto.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DruidColumnInfo
{
    private final String columnName;
    private final DruidColumnType dataType;

    @JsonCreator
    public DruidColumnInfo(
            @JsonProperty("COLUMN_NAME") String columnName,
            @JsonProperty("DATA_TYPE") DruidColumnType dataType)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.dataType = requireNonNull(dataType, "dataType is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public DruidColumnType getDataType()
    {
        return dataType;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("dataType", dataType)
                .toString();
    }
}
