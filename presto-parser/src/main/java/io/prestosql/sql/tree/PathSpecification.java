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
package io.prestosql.sql.tree;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class PathSpecification
        extends Node
{
    private List<PathElement> path;

    public PathSpecification(NodeLocation location, List<PathElement> path)
    {
        this(Optional.of(location), path);
    }

    @VisibleForTesting
    public PathSpecification(Optional<NodeLocation> location, List<PathElement> path)
    {
        super(location);
        this.path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
    }

    public List<PathElement> getPath()
    {
        return path;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPathSpecification(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return path;
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
        PathSpecification o = (PathSpecification) obj;
        return Objects.equals(path, o.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path);
    }

    @Override
    public String toString()
    {
        return Joiner.on(", ").join(path);
    }
}
