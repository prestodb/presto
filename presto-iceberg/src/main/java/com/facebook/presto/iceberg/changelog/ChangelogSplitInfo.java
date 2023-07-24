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
package com.facebook.presto.iceberg.changelog;

import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.ChangelogOperation;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ChangelogSplitInfo
{
    private final ChangelogOperation operation;
    private final long changeOrdinal;
    private final long snapshotId;
    private final String primaryKeyColumnName;
    private final List<IcebergColumnHandle> icebergColumns;

    @JsonCreator
    public ChangelogSplitInfo(
            @JsonProperty("operation") ChangelogOperation operation,
            @JsonProperty("changeOrdinal") long changeOrdinal,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("primaryKeyColumnName") String primaryKeyColumnName,
            @JsonProperty("icebergColumns") List<IcebergColumnHandle> icebergColumns)
    {
        this.operation = requireNonNull(operation, "operation is null");
        this.changeOrdinal = changeOrdinal;
        this.snapshotId = snapshotId;
        this.primaryKeyColumnName = requireNonNull(primaryKeyColumnName, "primaryKeyColumnName is null");
        this.icebergColumns = requireNonNull(icebergColumns, "icebergColumns is null");
    }

    @JsonProperty
    public ChangelogOperation getOperation()
    {
        return operation;
    }

    @JsonProperty
    public long getChangeOrdinal()
    {
        return changeOrdinal;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public String getPrimaryKeyColumnName()
    {
        return primaryKeyColumnName;
    }

    @JsonProperty
    public List<IcebergColumnHandle> getIcebergColumns()
    {
        return icebergColumns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(operation)
                .addValue(changeOrdinal)
                .addValue(snapshotId)
                .addValue(primaryKeyColumnName)
                .addValue(icebergColumns)
                .toString();
    }
}
