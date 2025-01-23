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
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class BindCodeGenerator
        implements SpecialFormBytecodeGenerator
{
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;
    private final Class lambdaInterface;

    public BindCodeGenerator(Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap, Class lambdaInterface)
    {
        this.compiledLambdaMap = compiledLambdaMap;
        this.lambdaInterface = lambdaInterface;
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlockVariable)
    {
        // Bind expression is used to generate captured lambda.
        // It takes the captured values and the uncaptured lambda, and produces captured lambda as the output.
        // The uncaptured lambda is just a method, and does not have a stack representation during execution.
        // As a result, the bind expression generates the captured lambda in one step.

        // outputBlockVariable cannot present because
        // 1. bind cannot be in the top level of an expression
        // 2. lambda cannot be put into blocks.
        checkArgument(!outputBlockVariable.isPresent());

        int numCaptures = arguments.size() - 1;
        LambdaDefinitionExpression lambda = (LambdaDefinitionExpression) arguments.get(numCaptures);
        checkState(compiledLambdaMap.containsKey(lambda), "lambda expressions map does not contain this lambda definition");
        CompiledLambda compiledLambda = compiledLambdaMap.get(lambda);

        return LambdaBytecodeGenerator.generateLambda(
                context,
                arguments.subList(0, numCaptures),
                compiledLambda,
                lambdaInterface);
    }
}
