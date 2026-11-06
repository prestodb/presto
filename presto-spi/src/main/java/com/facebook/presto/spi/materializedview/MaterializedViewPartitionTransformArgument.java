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
package com.facebook.presto.spi.materializedview;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.ConstantExpression;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * One argument of a materialized view partition transform. An argument is either the source
 * partition column or a constant value stored with the materialized view definition.
 */
public final class MaterializedViewPartitionTransformArgument
{
    private final Type type;
    private final Optional<ConstantExpression> constant;

    private MaterializedViewPartitionTransformArgument(Type type, Optional<ConstantExpression> constant)
    {
        this.type = requireNonNull(type, "type is null");
        this.constant = requireNonNull(constant, "constant is null");
    }

    public static MaterializedViewPartitionTransformArgument sourceColumn(Type type)
    {
        return new MaterializedViewPartitionTransformArgument(type, Optional.empty());
    }

    public static MaterializedViewPartitionTransformArgument constant(ConstantExpression constant)
    {
        requireNonNull(constant, "constant is null");
        return new MaterializedViewPartitionTransformArgument(constant.getType(), Optional.of(constant));
    }

    public Type getType()
    {
        return type;
    }

    public boolean isSourceColumn()
    {
        return !constant.isPresent();
    }

    public Optional<ConstantExpression> getConstant()
    {
        return constant;
    }
}
