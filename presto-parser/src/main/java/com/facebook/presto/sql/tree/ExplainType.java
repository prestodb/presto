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

import com.facebook.presto.common.SourceLocation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ExplainType
        extends ExplainOption
{
    public enum Type
    {
        LOGICAL,
        DISTRIBUTED,
        VALIDATE,
        IO
    }

    private final Type type;

    public ExplainType(Type type)
    {
        this(Optional.empty(), type);
    }

    public ExplainType(SourceLocation location, Type type)
    {
        this(Optional.of(location), type);
    }

    private ExplainType(Optional<SourceLocation> location, Type type)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type);
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
        ExplainType o = (ExplainType) obj;
        return Objects.equals(type, o.type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
