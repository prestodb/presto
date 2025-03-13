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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.JavaScalarFunctionImplementation;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.BytecodeUtils.OutputBlockVariableAndType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentType.VALUE_TYPE;
import static com.facebook.presto.sql.gen.BytecodeUtils.getAllScalarFunctionImplementationChoices;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class FunctionCallCodeGenerator
{
    public BytecodeNode generateCall(FunctionHandle functionHandle, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        FunctionAndTypeManager functionAndTypeManager = context.getFunctionManager();

        ScalarFunctionImplementation function = functionAndTypeManager.getScalarFunctionImplementation(functionHandle);
        checkArgument(function instanceof JavaScalarFunctionImplementation, format("FunctionCallCodeGenerator only handles JavaScalarFunctionImplementation, get %s", function.getClass().getName()));
        JavaScalarFunctionImplementation javaFunction = (JavaScalarFunctionImplementation) function;

        List<BytecodeNode> argumentsBytecode = new ArrayList<>();
        ScalarFunctionImplementationChoice choice = getAllScalarFunctionImplementationChoices(javaFunction).get(0);
        for (int i = 0; i < arguments.size(); i++) {
            RowExpression argument = arguments.get(i);
            ArgumentProperty argumentProperty = choice.getArgumentProperty(i);
            if (argumentProperty.getArgumentType() == VALUE_TYPE) {
                argumentsBytecode.add(context.generate(argument, Optional.empty()));
            }
            else {
                argumentsBytecode.add(context.generate(argument, Optional.empty(), Optional.of(argumentProperty.getLambdaInterface())));
            }
        }

        return context.generateCall(
                functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName(),
                javaFunction,
                argumentsBytecode,
                outputBlockVariable.map(variable -> new OutputBlockVariableAndType(variable, returnType)));
    }
}
