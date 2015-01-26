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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class Select
        extends Node
{
    private final boolean distinct;
    private final List<SelectItem> selectItems;
    private final List<QualifiedName> targets;

    public Select(boolean distinct, List<SelectItem> selectItems, List<QualifiedName> targets)
    {
        this.distinct = distinct;
        this.selectItems = ImmutableList.copyOf(checkNotNull(selectItems, "selectItems"));
        this.targets = ImmutableList.copyOf(checkNotNull(targets, "targets"));
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }

    public List<QualifiedName> getTargets()
    {
        return targets;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSelect(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("distinct", distinct)
                .add("selectItems", selectItems)
                .add("targets", targets)
                .omitNullValues()
                .toString();
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

        Select select = (Select) o;
        return distinct == select.distinct &&
                Objects.equals(selectItems, select.selectItems) &&
                Objects.equals(targets, select.targets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(distinct, selectItems, targets);
    }
}
