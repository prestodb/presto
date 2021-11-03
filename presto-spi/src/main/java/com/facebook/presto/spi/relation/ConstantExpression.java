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
package com.facebook.presto.spi.relation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Primitives;
import com.facebook.presto.common.predicate.Utils;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public final class ConstantExpression
        extends RowExpression
{
    private final Object value;
    private final Type type;

    public ConstantExpression(Object value, Type type)
    {
        requireNonNull(type, "type is null");
        if (value != null && !Primitives.wrap(type.getJavaType()).isInstance(value)) {
            throw new IllegalArgumentException(String.format("Object '%s' does not match type %s", value, type.getJavaType()));
        }
        this.value = value;
        this.type = type;
    }

    @JsonCreator
    public static ConstantExpression createConstantExpression(
            @JsonProperty("valueBlock") Block valueBlock,
            @JsonProperty("type") Type type)
    {
        return new ConstantExpression(Utils.blockToNativeValue(type, valueBlock), type);
    }

    @JsonProperty
    public Block getValueBlock()
    {
        return Utils.nativeValueToBlock(type, value);
    }

    public Object getValue()
    {
        return value;
    }

    public boolean isNull()
    {
        return value == null;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return String.valueOf(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ConstantExpression other = (ConstantExpression) obj;
        return Objects.equals(this.value, other.value) && Objects.equals(this.type, other.type);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitConstant(this, context);
    }
}
