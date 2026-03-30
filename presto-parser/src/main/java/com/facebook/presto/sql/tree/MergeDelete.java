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
import java.util.Optional;

public class MergeDelete
        extends MergeCase
{
    public MergeDelete()
    {
        this(Optional.empty());
    }

    public MergeDelete(NodeLocation location)
    {
        this(Optional.of(location));
    }

    public MergeDelete(Optional<NodeLocation> location)
    {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMergeDelete(this, context);
    }

    @Override
    public List<Identifier> getSetColumns()
    {
        return ImmutableList.of();
    }

    @Override
    public List<Expression> getSetExpressions()
    {
        return ImmutableList.of();
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return MergeDelete.class.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public String toString()
    {
        return "MergeDelete{}";
    }
}
