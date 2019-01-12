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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetPath
        extends Statement
{
    private final PathSpecification pathSpecification;

    public SetPath(PathSpecification pathSpecification)
    {
        this(Optional.empty(), pathSpecification);
    }

    public SetPath(NodeLocation location, PathSpecification pathSpecification)
    {
        this(Optional.of(location), pathSpecification);
    }

    private SetPath(Optional<NodeLocation> location, PathSpecification pathSpecification)
    {
        super(location);
        this.pathSpecification = requireNonNull(pathSpecification, "path is null");
    }

    public PathSpecification getPathSpecification()
    {
        return pathSpecification;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetPath(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(pathSpecification);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pathSpecification);
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
        SetPath o = (SetPath) obj;
        return Objects.equals(pathSpecification, o.pathSpecification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("pathSpecification", pathSpecification)
                .toString();
    }
}
