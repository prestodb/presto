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

public class CreateFunction
        extends Statement
{
    private final QualifiedName functionName;
    private final boolean replace;
    private final boolean temporary;
    private final List<SqlParameterDeclaration> parameters;
    private final String returnType;
    private final Optional<String> comment;
    private final RoutineCharacteristics characteristics;
    private final RoutineBody body;

    public CreateFunction(QualifiedName functionName, boolean replace, boolean temporary, List<SqlParameterDeclaration> parameters, String returnType, Optional<String> comment, RoutineCharacteristics characteristics, RoutineBody body)
    {
        this(Optional.empty(), replace, temporary, functionName, parameters, returnType, comment, characteristics, body);
    }

    public CreateFunction(NodeLocation location, boolean replace, boolean temporary, QualifiedName functionName, List<SqlParameterDeclaration> parameters, String returnType, Optional<String> comment, RoutineCharacteristics characteristics, RoutineBody body)
    {
        this(Optional.of(location), replace, temporary, functionName, parameters, returnType, comment, characteristics, body);
    }

    private CreateFunction(Optional<NodeLocation> location, boolean replace, boolean temporary, QualifiedName functionName, List<SqlParameterDeclaration> parameters, String returnType, Optional<String> comment, RoutineCharacteristics characteristics, RoutineBody body)
    {
        super(location);
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.replace = replace;
        this.temporary = temporary;
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.characteristics = requireNonNull(characteristics, "routineCharacteristics is null");
        this.body = requireNonNull(body, "body is null");
    }

    public QualifiedName getFunctionName()
    {
        return functionName;
    }

    public boolean isReplace()
    {
        return replace;
    }

    public boolean isTemporary()
    {
        return temporary;
    }

    public List<SqlParameterDeclaration> getParameters()
    {
        return parameters;
    }

    public String getReturnType()
    {
        return returnType;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public RoutineCharacteristics getCharacteristics()
    {
        return characteristics;
    }

    public RoutineBody getBody()
    {
        return body;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateFunction(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(body)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, temporary, parameters, returnType, comment, characteristics, body);
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
        CreateFunction o = (CreateFunction) obj;
        return Objects.equals(functionName, o.functionName) &&
                Objects.equals(temporary, o.temporary) &&
                Objects.equals(parameters, o.parameters) &&
                Objects.equals(returnType, o.returnType) &&
                Objects.equals(comment, o.comment) &&
                Objects.equals(characteristics, o.characteristics) &&
                Objects.equals(body, o.body);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("functionName", functionName)
                .add("temporary", temporary)
                .add("parameters", parameters)
                .add("returnType", returnType)
                .add("comment", comment)
                .add("characteristics", characteristics)
                .add("body", body)
                .toString();
    }
}
