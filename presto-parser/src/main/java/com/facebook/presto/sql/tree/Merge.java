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
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class Merge
        extends Statement
{
    private final Relation target;
    private final Relation source;
    private final Expression predicate;
    private final List<MergeCase> mergeCases;

    public Merge(
            NodeLocation location,
            Relation target,
            Relation source,
            Expression predicate,
            List<MergeCase> mergeCases)
    {
        this(Optional.of(location), target, source, predicate, mergeCases);
    }

    public Merge(
            Optional<NodeLocation> location,
            Relation target,
            Relation source,
            Expression predicate,
            List<MergeCase> mergeCases)
    {
        super(location);
        this.target = requireNonNull(target, "target is null");
        checkArgument(target instanceof Table || target instanceof AliasedRelation, "target (%s) is neither a Table nor an AliasedRelation", target);
        this.source = requireNonNull(source, "source is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.mergeCases = ImmutableList.copyOf(requireNonNull(mergeCases, "mergeCases is null"));
    }

    public Relation getTarget()
    {
        return target;
    }

    public Relation getSource()
    {
        return source;
    }

    public Expression getPredicate()
    {
        return predicate;
    }

    public List<MergeCase> getMergeCases()
    {
        return mergeCases;
    }

    public Table getTargetTable()
    {
        if (target instanceof Table) {
            return (Table) target;
        }
        checkArgument(target instanceof AliasedRelation, "MERGE relation is neither a Table nor an AliasedRelation");
        return (Table) ((AliasedRelation) target).getRelation();
    }

    public Optional<Identifier> getTargetAlias()
    {
        if (target instanceof AliasedRelation) {
            return Optional.of(((AliasedRelation) target).getAlias());
        }
        return Optional.empty();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMerge(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.builder();
        builder.add(target);
        builder.add(source);
        builder.add(predicate);
        builder.addAll(mergeCases);
        return builder.build();
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
        Merge merge = (Merge) o;
        return Objects.equals(target, merge.target) &&
                Objects.equals(source, merge.source) &&
                Objects.equals(predicate, merge.predicate) &&
                Objects.equals(mergeCases, merge.mergeCases);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, source, predicate, mergeCases);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("target", target)
                .add("relation", source)
                .add("predicate", predicate)
                .add("mergeCases", mergeCases)
                .omitNullValues()
                .toString();
    }
}
