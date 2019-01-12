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

public class ShowTables
        extends Statement
{
    private final Optional<QualifiedName> schema;
    private final Optional<String> likePattern;
    private final Optional<String> escape;

    public ShowTables(Optional<QualifiedName> schema, Optional<String> likePattern, Optional<String> escape)
    {
        this(Optional.empty(), schema, likePattern, escape);
    }

    public ShowTables(NodeLocation location, Optional<QualifiedName> schema, Optional<String> likePattern, Optional<String> escape)
    {
        this(Optional.of(location), schema, likePattern, escape);
    }

    private ShowTables(Optional<NodeLocation> location, Optional<QualifiedName> schema, Optional<String> likePattern, Optional<String> escape)
    {
        super(location);
        requireNonNull(schema, "schema is null");
        requireNonNull(likePattern, "likePattern is null");
        requireNonNull(escape, "escape is null");

        this.schema = schema;
        this.likePattern = likePattern;
        this.escape = escape;
    }

    public Optional<QualifiedName> getSchema()
    {
        return schema;
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
        return visitor.visitShowTables(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, likePattern, escape);
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
        ShowTables o = (ShowTables) obj;
        return Objects.equals(schema, o.schema) &&
                Objects.equals(likePattern, o.likePattern) &&
                Objects.equals(escape, o.escape);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", schema)
                .add("likePattern", likePattern)
                .add("escape", escape)
                .toString();
    }
}
