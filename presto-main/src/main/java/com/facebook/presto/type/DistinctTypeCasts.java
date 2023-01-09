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
package com.facebook.presto.type;

import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.DistinctType.hasAncestorRelationship;
import static com.facebook.presto.common.type.StandardTypes.DISTINCT_TYPE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.identity;

public class DistinctTypeCasts
{
    private DistinctTypeCasts() {}

    public static final DistinctTypeToCast DISTINCT_TYPE_TO_CAST = new DistinctTypeToCast();
    public static final DistinctTypeFromCast DISTINCT_TYPE_FROM_CAST = new DistinctTypeFromCast();

    public static class DistinctTypeToCast
            extends SqlOperator
    {
        private DistinctTypeToCast()
        {
            super(CAST,
                    ImmutableList.of(typeVariable("T1"), withVariadicBound("T2", DISTINCT_TYPE)),
                    ImmutableList.of(),
                    parseTypeSignature("T2"),
                    ImmutableList.of(parseTypeSignature("T1")));
        }

        @Override
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type fromType = boundVariables.getTypeVariable("T1");
            Type toType = boundVariables.getTypeVariable("T2");
            checkCanCast(fromType, toType);
            return new BuiltInScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    identity(fromType.getJavaType()));
        }
    }

    public static class DistinctTypeFromCast
            extends SqlOperator
    {
        private DistinctTypeFromCast()
        {
            super(CAST,
                    ImmutableList.of(typeVariable("T2"), withVariadicBound("T1", DISTINCT_TYPE)),
                    ImmutableList.of(),
                    parseTypeSignature("T2"),
                    ImmutableList.of(parseTypeSignature("T1")));
        }

        @Override
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type fromType = boundVariables.getTypeVariable("T1");
            Type toType = boundVariables.getTypeVariable("T2");
            checkCanCast(fromType, toType);
            return new BuiltInScalarFunctionImplementation(
                    false,
                    ImmutableList.of(
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                            valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    identity(fromType.getJavaType()));
        }
    }

    private static void checkCanCast(Type fromType, Type toType)
    {
        if (fromType instanceof DistinctType && toType instanceof DistinctType) {
            DistinctType fromDistinctType = (DistinctType) fromType;
            DistinctType toDistinctType = (DistinctType) toType;

            if (!hasAncestorRelationship(fromDistinctType, toDistinctType)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", fromDistinctType.getName(), toDistinctType.getName()));
            }
        }
        else if (fromType instanceof DistinctType) {
            DistinctType fromDistinctType = (DistinctType) fromType;
            if (!fromDistinctType.getBaseType().equals(toType)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", fromDistinctType.getName(), toType));
            }
        }
        else if (toType instanceof DistinctType) {
            DistinctType toDistinctType = (DistinctType) toType;
            if (!toDistinctType.getBaseType().equals(fromType)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", fromType, toDistinctType.getName()));
            }
        }
    }
}
