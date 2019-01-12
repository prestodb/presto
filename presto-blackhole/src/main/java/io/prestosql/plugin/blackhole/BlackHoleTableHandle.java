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
package io.prestosql.plugin.blackhole;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public final class BlackHoleTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<BlackHoleColumnHandle> columnHandles;
    private final int splitCount;
    private final int pagesPerSplit;
    private final int rowsPerPage;
    private final int fieldsLength;
    private final Duration pageProcessingDelay;

    public BlackHoleTableHandle(
            ConnectorTableMetadata tableMetadata,
            int splitCount,
            int pagesPerSplit,
            int rowsPerPage,
            int fieldsLength,
            Duration pageProcessingDelay)
    {
        this(tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream()
                        .map(BlackHoleColumnHandle::new)
                        .collect(toList()),
                splitCount,
                pagesPerSplit,
                rowsPerPage,
                fieldsLength,
                pageProcessingDelay);
    }

    @JsonCreator
    public BlackHoleTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnHandles") List<BlackHoleColumnHandle> columnHandles,
            @JsonProperty("splitCount") int splitCount,
            @JsonProperty("pagesPerSplit") int pagesPerSplit,
            @JsonProperty("rowsPerPage") int rowsPerPage,
            @JsonProperty("fieldsLength") int fieldsLength,
            @JsonProperty("pageProcessingDelay") Duration pageProcessingDelay)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columnHandles = columnHandles;
        this.splitCount = splitCount;
        this.pagesPerSplit = pagesPerSplit;
        this.rowsPerPage = rowsPerPage;
        this.fieldsLength = fieldsLength;
        this.pageProcessingDelay = pageProcessingDelay;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<BlackHoleColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    @JsonProperty
    public int getSplitCount()
    {
        return splitCount;
    }

    @JsonProperty
    public int getPagesPerSplit()
    {
        return pagesPerSplit;
    }

    @JsonProperty
    public int getRowsPerPage()
    {
        return rowsPerPage;
    }

    @JsonProperty
    public int getFieldsLength()
    {
        return fieldsLength;
    }

    @JsonProperty
    public Duration getPageProcessingDelay()
    {
        return pageProcessingDelay;
    }

    public ConnectorTableMetadata toTableMetadata()
    {
        return new ConnectorTableMetadata(
                toSchemaTableName(),
                columnHandles.stream().map(BlackHoleColumnHandle::toColumnMetadata).collect(toList()));
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSchemaName(), getTableName());
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
        BlackHoleTableHandle other = (BlackHoleTableHandle) obj;
        return Objects.equals(this.getSchemaName(), other.getSchemaName()) &&
                Objects.equals(this.getTableName(), other.getTableName());
    }
}
