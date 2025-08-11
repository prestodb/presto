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

public class ShowFunctions
        extends Statement
{
    private final Optional<String> likePattern;
    private final Optional<String> escape;

    public ShowFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        this(Optional.empty(), likePattern, escape);
    }

    public ShowFunctions(NodeLocation location, Optional<String> likePattern, Optional<String> escape)
    {
        this(Optional.of(location), likePattern, escape);
    }

    private ShowFunctions(Optional<NodeLocation> location, Optional<String> likePattern, Optional<String> escape)
    {
        super(location);
        this.likePattern = requireNonNull(likePattern, "likePattern is null");
        this.escape = requireNonNull(escape, "escape is null");
    }

    public Optional<String> getLikePattern()
    {
        return likePattern;
    }

    public Optional<String> getEscape()
    {
        return escape;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowFunctions(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(likePattern, escape);
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
        ShowFunctions o = (ShowFunctions) obj;
        return Objects.equals(likePattern, o.likePattern) &&
                Objects.equals(escape, o.escape);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("likePattern", likePattern)
                .add("escape", escape)
                .toString();
    }
}
