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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateBranch
        extends Statement
{
    private final QualifiedName tableName;
    private final String branchName;
    private final Optional<Long> snapshotId;
    private final Optional<Expression> asOfTimestamp;
    private final Optional<Long> retainDays;
    private final Optional<Integer> minSnapshotsToKeep;
    private final Optional<Long> maxSnapshotAgeDays;

    public CreateBranch(
            QualifiedName tableName,
            String branchName,
            Optional<Long> snapshotId,
            Optional<Expression> asOfTimestamp,
            Optional<Long> retainDays,
            Optional<Integer> minSnapshotsToKeep,
            Optional<Long> maxSnapshotAgeDays)
    {
        this(Optional.empty(), tableName, branchName, snapshotId, asOfTimestamp, retainDays, minSnapshotsToKeep, maxSnapshotAgeDays);
    }

    public CreateBranch(
            NodeLocation location,
            QualifiedName tableName,
            String branchName,
            Optional<Long> snapshotId,
            Optional<Expression> asOfTimestamp,
            Optional<Long> retainDays,
            Optional<Integer> minSnapshotsToKeep,
            Optional<Long> maxSnapshotAgeDays)
    {
        this(Optional.of(location), tableName, branchName, snapshotId, asOfTimestamp, retainDays, minSnapshotsToKeep, maxSnapshotAgeDays);
    }

    private CreateBranch(
            Optional<NodeLocation> location,
            QualifiedName tableName,
            String branchName,
            Optional<Long> snapshotId,
            Optional<Expression> asOfTimestamp,
            Optional<Long> retainDays,
            Optional<Integer> minSnapshotsToKeep,
            Optional<Long> maxSnapshotAgeDays)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.branchName = requireNonNull(branchName, "branchName is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.asOfTimestamp = requireNonNull(asOfTimestamp, "asOfTimestamp is null");
        this.retainDays = requireNonNull(retainDays, "retainDays is null");
        this.minSnapshotsToKeep = requireNonNull(minSnapshotsToKeep, "minSnapshotsToKeep is null");
        this.maxSnapshotAgeDays = requireNonNull(maxSnapshotAgeDays, "maxSnapshotAgeDays is null");
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public String getBranchName()
    {
        return branchName;
    }

    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    public Optional<Expression> getAsOfTimestamp()
    {
        return asOfTimestamp;
    }

    public Optional<Long> getRetainDays()
    {
        return retainDays;
    }

    public Optional<Integer> getMinSnapshotsToKeep()
    {
        return minSnapshotsToKeep;
    }

    public Optional<Long> getMaxSnapshotAgeDays()
    {
        return maxSnapshotAgeDays;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateBranch(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        asOfTimestamp.ifPresent(children::add);
        return children.build();
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
        CreateBranch that = (CreateBranch) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(branchName, that.branchName) &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(asOfTimestamp, that.asOfTimestamp) &&
                Objects.equals(retainDays, that.retainDays) &&
                Objects.equals(minSnapshotsToKeep, that.minSnapshotsToKeep) &&
                Objects.equals(maxSnapshotAgeDays, that.maxSnapshotAgeDays);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, branchName, snapshotId, asOfTimestamp, retainDays, minSnapshotsToKeep, maxSnapshotAgeDays);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableName)
                .add("branchName", branchName)
                .add("snapshotId", snapshotId)
                .add("asOfTimestamp", asOfTimestamp)
                .add("retainDays", retainDays)
                .add("minSnapshotsToKeep", minSnapshotsToKeep)
                .add("maxSnapshotAgeDays", maxSnapshotAgeDays)
                .toString();
    }
}
