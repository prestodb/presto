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
    private final long ordinal;
    private final long snapshotId;
    private final List<IcebergColumnHandle> icebergColumns;

    @JsonCreator
    public ChangelogSplitInfo(
            @JsonProperty("operation") ChangelogOperation operation,
            @JsonProperty("ordinal") long ordinal,
            @JsonProperty("snapshotId") long snapshotId,
            @JsonProperty("icebergColumns") List<IcebergColumnHandle> icebergColumns)
    {
        this.operation = requireNonNull(operation, "operation is null");
        this.ordinal = ordinal;
        this.snapshotId = snapshotId;
        this.icebergColumns = requireNonNull(icebergColumns, "icebergColumns is null");
    }

    @JsonProperty
    public ChangelogOperation getOperation()
    {
        return operation;
    }

    @JsonProperty
    public long getOrdinal()
    {
        return ordinal;
    }

    @JsonProperty
    public long getSnapshotId()
    {
        return snapshotId;
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
                .addValue(ordinal)
                .addValue(snapshotId)
                .addValue(icebergColumns)
                .toString();
    }
}
