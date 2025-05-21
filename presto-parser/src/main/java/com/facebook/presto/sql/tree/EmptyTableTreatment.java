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

public final class EmptyTableTreatment
        extends Node
{
    private final Treatment treatment;

    public EmptyTableTreatment(NodeLocation location, Treatment treatment)
    {
        super(Optional.of(location));
        this.treatment = requireNonNull(treatment, "treatment is null");
    }

    public Treatment getTreatment()
    {
        return treatment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitEmptyTableTreatment(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        return treatment == ((EmptyTableTreatment) obj).treatment;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(treatment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("treatment", treatment)
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return treatment == ((EmptyTableTreatment) other).treatment;
    }

    public enum Treatment
    {
        KEEP, PRUNE
    }
}
