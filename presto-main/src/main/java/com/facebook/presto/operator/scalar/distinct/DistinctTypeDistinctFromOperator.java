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
package com.facebook.presto.operator.scalar.distinct;

import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionInvoker;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.InvocationConvention;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DISTINCT_TYPE;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ReturnPlaceConvention;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class DistinctTypeDistinctFromOperator
        extends SqlOperator
{
    public static final DistinctTypeDistinctFromOperator DISTINCT_TYPE_DISTINCT_FROM_OPERATOR = new DistinctTypeDistinctFromOperator();

    private DistinctTypeDistinctFromOperator()
    {
        super(IS_DISTINCT_FROM,
                ImmutableList.of(withVariadicBound("T", DISTINCT_TYPE)),
                ImmutableList.of(),
                parseTypeSignature(BOOLEAN),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("T")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        DistinctType type = (DistinctType) boundVariables.getTypeVariable("T");
        Type baseType = type.getBaseType();
        FunctionHandle functionHandle = functionAndTypeManager.resolveOperator(IS_DISTINCT_FROM, fromTypes(baseType, baseType));
        FunctionInvoker nullFlagfunctionInvoker = functionAndTypeManager.getFunctionInvokerProvider().createFunctionInvoker(
                functionHandle,
                Optional.of(new InvocationConvention(
                        ImmutableList.of(NULL_FLAG, NULL_FLAG),
                        FAIL_ON_NULL,
                        false)));
        FunctionInvoker blockPositionfunctionInvoker = functionAndTypeManager.getFunctionInvokerProvider().createFunctionInvoker(
                functionHandle,
                Optional.of(new InvocationConvention(
                        ImmutableList.of(BLOCK_POSITION, BLOCK_POSITION),
                        FAIL_ON_NULL,
                        false)));

        return new BuiltInScalarFunctionImplementation(
                ImmutableList.of(
                    new ScalarFunctionImplementationChoice(
                            false,
                            ImmutableList.of(valueTypeArgumentProperty(NULL_FLAG), valueTypeArgumentProperty(NULL_FLAG)),
                            ReturnPlaceConvention.STACK,
                            nullFlagfunctionInvoker.methodHandle(),
                            Optional.empty()),
                    new ScalarFunctionImplementationChoice(
                            false,
                            ImmutableList.of(valueTypeArgumentProperty(BLOCK_AND_POSITION), valueTypeArgumentProperty(BLOCK_AND_POSITION)),
                            ReturnPlaceConvention.STACK,
                            blockPositionfunctionInvoker.methodHandle(),
                            Optional.empty())));
    }
}
