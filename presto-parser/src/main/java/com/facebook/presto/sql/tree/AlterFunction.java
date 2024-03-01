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

public class AlterFunction
        extends Statement
{
    private final QualifiedName functionName;
    private final Optional<List<String>> parameterTypes;
    private final AlterRoutineCharacteristics characteristics;

    public AlterFunction(QualifiedName functionName, Optional<List<String>> parameterTypes, AlterRoutineCharacteristics characteristics)
    {
        this(Optional.empty(), functionName, parameterTypes, characteristics);
    }

    public AlterFunction(NodeLocation location, QualifiedName functionName, Optional<List<String>> parameterTypes, AlterRoutineCharacteristics characteristics)
    {
        this(Optional.of(location), functionName, parameterTypes, characteristics);
    }

    private AlterFunction(Optional<NodeLocation> location, QualifiedName functionName, Optional<List<String>> parameterTypes, AlterRoutineCharacteristics characteristics)
    {
        super(location);
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.parameterTypes = requireNonNull(parameterTypes, "parameterTypes is null");
        this.characteristics = requireNonNull(characteristics, "characteristics is null");
    }

    public QualifiedName getFunctionName()
    {
        return functionName;
    }

    public Optional<List<String>> getParameterTypes()
    {
        return parameterTypes;
    }

    public AlterRoutineCharacteristics getCharacteristics()
    {
        return characteristics;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitAlterFunction(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, parameterTypes, characteristics);
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
        AlterFunction o = (AlterFunction) obj;
        return Objects.equals(functionName, o.functionName) &&
                Objects.equals(parameterTypes, o.parameterTypes) &&
                Objects.equals(characteristics, o.characteristics);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("functionName", functionName)
                .add("parameterTypes", parameterTypes)
                .add("characteristics", characteristics)
                .toString();
    }
}
