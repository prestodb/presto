

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

import static java.util.Objects.requireNonNull;

public class Trim
        extends Expression
{
    private final Specification specification;
    private final Expression trimSource;
    private final Optional<Expression> trimCharacter;

    public Trim(Specification specification, Expression trimSource, Optional<Expression> trimCharacter)
    {
        this(Optional.empty(), specification, trimSource, trimCharacter);
    }

    public Trim(NodeLocation location, Specification specification, Expression trimSource, Optional<Expression> trimCharacter)
    {
        this(Optional.of(location), specification, trimSource, trimCharacter);
    }

    private Trim(Optional<NodeLocation> location, Specification specification, Expression trimSource, Optional<Expression> trimCharacter)
    {
        super(location);
        this.specification = requireNonNull(specification, "specification is null");
        this.trimSource = requireNonNull(trimSource, "trimSource is null");
        this.trimCharacter = requireNonNull(trimCharacter, "trimCharacter is null");
    }

    public Specification getSpecification()
    {
        return specification;
    }

    public Expression getTrimSource()
    {
        return trimSource;
    }

    public Optional<Expression> getTrimCharacter()
    {
        return trimCharacter;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitTrim(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(trimSource);
        trimCharacter.ifPresent(nodes::add);
        return nodes.build();
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

        Trim that = (Trim) o;
        return specification == that.specification &&
                Objects.equals(trimSource, that.trimSource) &&
                Objects.equals(trimCharacter, that.trimCharacter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(specification, trimSource, trimCharacter);
    }

    public enum Specification
    {
        BOTH("trim"),
        LEADING("ltrim"),
        TRAILING("rtrim");

        private final String functionName;

        Specification(String functionName)
        {
            this.functionName = requireNonNull(functionName, "functionName is null");
        }

        public String getFunctionName()
        {
            return functionName;
        }
    }
}
