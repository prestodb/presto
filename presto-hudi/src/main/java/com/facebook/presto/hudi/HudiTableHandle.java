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

package com.facebook.presto.hudi;

import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.util.Lazy;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String path;
    private final HudiTableType hudiTableType;
    private final transient Optional<Table> table;
    private final transient Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient;
    private final transient Lazy<String> lazyLatestCommitTime;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("path") String path,
            @JsonProperty("tableType") HudiTableType hudiTableType,
            @JsonProperty("latestCommitTime") String latestCommitTime)
    {
        this(Optional.empty(), Optional.empty(), schemaName, tableName, path, hudiTableType, () -> latestCommitTime);
    }

    public HudiTableHandle(
            Table table,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            String schemaName,
            String tableName,
            String path,
            HudiTableType hudiTableType)
    {
        this(
                Optional.of(table),
                Optional.of(lazyMetaClient),
                schemaName,
                tableName,
                path,
                hudiTableType,
                () -> lazyMetaClient
                        .get()
                        .getActiveTimeline()
                        .getCommitsTimeline()
                        .filterCompletedInstants()
                        .lastInstant()
                        .map(HoodieInstant::requestedTime)
                        .orElseThrow(() -> new PrestoException(
                                HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT,
                                "Table has no valid commits")));
    }

    HudiTableHandle(
            Optional<Table> table,
            Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient,
            String schemaName,
            String tableName,
            String path,
            HudiTableType hudiTableType,
            Supplier<String> latestCommitTimeSupplier)
    {
        this.table = requireNonNull(table, "table is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.path = requireNonNull(path, "path is null");
        this.hudiTableType = requireNonNull(hudiTableType, "tableType is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "lazyMetaClient is null");
        this.lazyLatestCommitTime = Lazy.lazily(latestCommitTimeSupplier);
    }

    public Table getTable()
    {
        checkArgument(table.isPresent(),
                "getTable() called on a table handle that has no metastore table object; "
                        + "this is likely because it is called on the worker.");
        return table.get();
    }

    public HoodieTableMetaClient getMetaClient()
    {
        checkArgument(lazyMetaClient.isPresent(),
                "getMetaClient() called on a table handle that has no Hudi meta-client; "
                        + "this is likely because it is called on the worker.");
        return lazyMetaClient.get().get();
    }

    @JsonProperty
    public String getLatestCommitTime()
    {
        return lazyLatestCommitTime.get();
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
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public HudiTableType getTableType()
    {
        return hudiTableType;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
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
        HudiTableHandle that = (HudiTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                hudiTableType == that.hudiTableType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, hudiTableType);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName;
    }
}
