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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class AddPartition
        extends Statement
{
    private final QualifiedName source;
    private final List<PartitionItem> partitionItemList;

    public AddPartition(QualifiedName source,
                        List<PartitionItem> partitionItemList)
    {
        this.source = checkNotNull(source, "source name is null");
        this.partitionItemList = ImmutableList.copyOf(partitionItemList);
    }

    public QualifiedName getSource()
    {
        return source;
    }
    public List<PartitionItem> getPartitionItemList()
    {
        return partitionItemList;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAddPartition(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(source, partitionItemList);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        AddPartition o = (AddPartition) obj;
        return Objects.equal(source, o.source) &&
                Objects.equal(partitionItemList, o.partitionItemList);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("source", source)
                .add("partitionItemList", partitionItemList)
                .toString();
    }
}
