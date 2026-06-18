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

public class DropBranch
        extends Statement
{
    private final QualifiedName tableName;
    private final String branchName;
    private final boolean tableExists;
    private final boolean branchExists;

    public DropBranch(QualifiedName tableName, String branchName, boolean tableExists, boolean branchExists)
    {
        this(Optional.empty(), tableName, branchName, tableExists, branchExists);
    }

    public DropBranch(NodeLocation location, QualifiedName tableName, String branchName, boolean tableExists, boolean branchExists)
    {
        this(Optional.of(location), tableName, branchName, tableExists, branchExists);
    }

    private DropBranch(Optional<NodeLocation> location, QualifiedName tableName, String branchName, boolean tableExists, boolean branchExists)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.branchName = requireNonNull(branchName, "branchName is null");
        this.tableExists = tableExists;
        this.branchExists = branchExists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public String getBranchName()
    {
        return branchName;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isBranchExists()
    {
        return branchExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropBranch(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        DropBranch that = (DropBranch) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(branchName, that.branchName) &&
                Objects.equals(tableExists, that.tableExists) &&
                Objects.equals(branchExists, that.branchExists);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, branchName, tableExists, branchExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableName)
                .add("branchName", branchName)
                .add("tableExists", tableExists)
                .add("branchExists", branchExists)
                .toString();
    }
}
