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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

public class TypeVariableConstraint
{
    private final String name;
    private final boolean comparableRequired;
    private final boolean orderableRequired;
    private final String variadicBound;
    private final Class<? extends Type> typeBound;

    @JsonCreator
    public TypeVariableConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("comparableRequired") boolean comparableRequired,
            @JsonProperty("orderableRequired") boolean orderableRequired,
            @JsonProperty("variadicBound") @Nullable String variadicBound,
            @JsonProperty("boundedBy") Class<? extends Type> typeBound)
    {
        this.name = name;
        this.comparableRequired = comparableRequired;
        this.orderableRequired = orderableRequired;
        this.variadicBound = variadicBound;
        this.typeBound = typeBound;
    }

    public TypeVariableConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("comparableRequired") boolean comparableRequired,
            @JsonProperty("orderableRequired") boolean orderableRequired,
            @JsonProperty("variadicBound") @Nullable String variadicBound)
    {
        this(name, comparableRequired, orderableRequired, variadicBound, Type.class);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isComparableRequired()
    {
        return comparableRequired;
    }

    @JsonProperty
    public boolean isOrderableRequired()
    {
        return orderableRequired;
    }

    @JsonProperty
    public String getVariadicBound()
    {
        return variadicBound;
    }

    @JsonProperty
    public Class<? extends Type> getTypeBound()
    {
        return typeBound;
    }

    public boolean canBind(Type type)
    {
        if (comparableRequired && !type.isComparable()) {
            return false;
        }
        if (orderableRequired && !type.isOrderable()) {
            return false;
        }
        if (!typeBound.isInstance(type)) {
            return false;
        }
        if (variadicBound != null && !type.getTypeSignature().getBase().equals(variadicBound)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        String value = name;
        if (comparableRequired) {
            value += ":comparable";
        }
        if (orderableRequired) {
            value += ":orderable";
        }
        if (variadicBound != null) {
            value += ":" + variadicBound + "<*>";
        }
        if (!typeBound.equals(Type.class)) {
            value += " extends " + typeBound.getSimpleName();
        }
        return value;
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
        TypeVariableConstraint that = (TypeVariableConstraint) o;
        return comparableRequired == that.comparableRequired &&
                orderableRequired == that.orderableRequired &&
                Objects.equals(name, that.name) &&
                Objects.equals(variadicBound, that.variadicBound) &&
                Objects.equals(typeBound, that.typeBound);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, comparableRequired, orderableRequired, variadicBound, typeBound);
    }
}
