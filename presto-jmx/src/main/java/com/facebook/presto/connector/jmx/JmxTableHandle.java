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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public class JmxTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName tableName;
    private final List<String> objectNames;
    private final List<JmxColumnHandle> columnHandles;
    private final boolean liveData;

    @JsonCreator
    public JmxTableHandle(
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("objectNames") List<String> objectNames,
            @JsonProperty("columnHandles") List<JmxColumnHandle> columnHandles,
            @JsonProperty("liveData") boolean liveData)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.objectNames = ImmutableList.copyOf(requireNonNull(objectNames, "objectName is null"));
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.liveData = liveData;

        checkArgument(!objectNames.isEmpty(), "objectsNames is empty");
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getObjectNames()
    {
        return objectNames;
    }

    @JsonProperty
    public List<JmxColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public boolean isLiveData()
    {
        return liveData;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, objectNames, columnHandles, liveData);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JmxTableHandle other = (JmxTableHandle) obj;
        return Objects.equals(tableName, other.tableName) &&
                Objects.equals(this.objectNames, other.objectNames) &&
                Objects.equals(this.columnHandles, other.columnHandles) &&
                Objects.equals(this.liveData, other.liveData);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", objectNames)
                .add("objectNames", objectNames)
                .add("columnHandles", columnHandles)
                .add("liveData", liveData)
                .toString();
    }

    public ConnectorTableMetadata getTableMetadata()
    {
        return new ConnectorTableMetadata(
                tableName,
                ImmutableList.copyOf(transform(columnHandles, JmxColumnHandle::getColumnMetadata)));
    }
}
