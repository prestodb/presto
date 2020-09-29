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
package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ScalarImplementationChoice;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.InvocationConvention;
import com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention;
import com.facebook.presto.spi.function.InvocationConvention.InvocationReturnConvention;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentType.FUNCTION_TYPE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.BLOCK_AND_POSITION;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class FunctionInvokerProvider
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public FunctionInvokerProvider(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    public FunctionInvoker createFunctionInvoker(FunctionHandle functionHandle, Optional<InvocationConvention> invocationConvention)
    {
        BuiltInScalarFunctionImplementation builtInScalarFunctionImplementation = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionHandle);
        for (ScalarImplementationChoice choice : builtInScalarFunctionImplementation.getAllChoices()) {
            if (checkChoice(choice.getArgumentProperties(), choice.isNullable(), choice.hasProperties(), invocationConvention)) {
                return new FunctionInvoker(choice.getMethodHandle());
            }
        }
        checkState(invocationConvention.isPresent());
        throw new PrestoException(FUNCTION_NOT_FOUND, format("Dependent function implementation (%s) with convention (%s) is not available", functionHandle, invocationConvention.toString()));
    }

    @VisibleForTesting
    static boolean checkChoice(List<ArgumentProperty> definitionArgumentProperties, boolean definitionReturnsNullable, boolean definitionHasSession, Optional<InvocationConvention> invocationConvention)
    {
        for (int i = 0; i < definitionArgumentProperties.size(); i++) {
            InvocationArgumentConvention invocationArgumentConvention = invocationConvention.get().getArgumentConvention(i);
            NullConvention nullConvention = definitionArgumentProperties.get(i).getNullConvention();
            // return false because function types do not have a null convention
            if (definitionArgumentProperties.get(i).getArgumentType() == FUNCTION_TYPE) {
                if (invocationArgumentConvention != InvocationArgumentConvention.FUNCTION) {
                    return false;
                }
                // Support can be added when this becomes necessary
                throw new UnsupportedOperationException("Invocation convention for function type is not supported");
            }
            if (nullConvention == RETURN_NULL_ON_NULL && invocationArgumentConvention != InvocationArgumentConvention.NEVER_NULL) {
                return false;
            }
            if (nullConvention == USE_BOXED_TYPE && invocationArgumentConvention != InvocationArgumentConvention.BOXED_NULLABLE) {
                return false;
            }
            if (nullConvention == USE_NULL_FLAG && invocationArgumentConvention != InvocationArgumentConvention.NULL_FLAG) {
                return false;
            }
            if (nullConvention == BLOCK_AND_POSITION && invocationArgumentConvention != InvocationArgumentConvention.BLOCK_POSITION) {
                return false;
            }
        }

        if (definitionReturnsNullable && invocationConvention.get().getReturnConvention() != InvocationReturnConvention.NULLABLE_RETURN) {
            return false;
        }
        if (!definitionReturnsNullable) {
            // For each of the arguments, the invocation convention is required to be FAIL_ON_NULL
            // when the  corresponding definition convention has RETURN_NULL_ON_NULL convention.
            // As a result, when `definitionReturnsNullable` is false, the function
            // can never return a null value. Therefore, the if below is sufficient.
            if (invocationConvention.get().getReturnConvention() != InvocationReturnConvention.FAIL_ON_NULL) {
                return false;
            }
        }
        if (definitionHasSession != invocationConvention.get().hasSession()) {
            return false;
        }
        return true;
    }
}
