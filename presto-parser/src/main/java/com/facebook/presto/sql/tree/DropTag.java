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

public class DropTag
        extends Statement
{
    private final QualifiedName tableName;
    private final String tagName;
    private final boolean tableExists;
    private final boolean tagExists;

    public DropTag(QualifiedName tableName, String tagName, boolean tableExists, boolean tagExists)
    {
        this(Optional.empty(), tableName, tagName, tableExists, tagExists);
    }

    public DropTag(NodeLocation location, QualifiedName tableName, String tagName, boolean tableExists, boolean tagExists)
    {
        this(Optional.of(location), tableName, tagName, tableExists, tagExists);
    }

    private DropTag(Optional<NodeLocation> location, QualifiedName tableName, String tagName, boolean tableExists, boolean tagExists)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "table is null");
        this.tagName = requireNonNull(tagName, "tagName is null");
        this.tableExists = tableExists;
        this.tagExists = tagExists;
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public String getTagName()
    {
        return tagName;
    }

    public boolean isTableExists()
    {
        return tableExists;
    }

    public boolean isTagExists()
    {
        return tagExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropTag(this, context);
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
        DropTag that = (DropTag) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(tagName, that.tagName) &&
                Objects.equals(tableExists, that.tableExists) &&
                Objects.equals(tagExists, that.tagExists);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, tagName, tableExists, tagExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableName)
                .add("tagName", tagName)
                .add("tableExists", tableExists)
                .add("tagExists", tagExists)
                .toString();
    }
}
