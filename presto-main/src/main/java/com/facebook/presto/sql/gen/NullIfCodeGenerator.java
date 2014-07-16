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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.control.IfStatement;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.sql.gen.ByteCodeUtils.generateFunctionCall;

public class NullIfCodeGenerator
        implements ByteCodeGenerator
{
    @Override
    public ByteCodeNode generateExpression(Signature signature, ByteCodeGeneratorContext generatorContext, Type returnType, List<RowExpression> arguments)
    {
        CompilerContext context = generatorContext.getContext();

        RowExpression first = arguments.get(0);
        RowExpression second = arguments.get(1);

        LabelNode notMatch = new LabelNode("notMatch");

        // push first arg on the stack
        Block block = new Block(context)
                .comment("check if first arg is null")
                .append(generatorContext.generate(first))
                .append(ByteCodeUtils.ifWasNullPopAndGoto(context, notMatch, void.class));

        Type firstType = first.getType();
        Type secondType = second.getType();

        // this is a hack! We shouldn't be determining type coercions at this point, but there's no way
        // around it in the current expression AST
        Type commonType = FunctionRegistry.getCommonSuperType(firstType, secondType).get();

        // if (equal(cast(first as <common type>), cast(second as <common type>))
        FunctionBinding functionBinding = generatorContext.getBootstrapBinder().bindOperator(
                OperatorType.EQUAL,
                generatorContext.generateGetSession(),
                ImmutableList.of(
                        cast(generatorContext, new Block(context).dup(firstType.getJavaType()), firstType, commonType),
                        cast(generatorContext, generatorContext.generate(second), secondType, commonType)),
                ImmutableList.of(firstType, secondType));

        MethodType methodType = functionBinding.getCallSite().type();
        Class<?> unboxedReturnType = Primitives.unwrap(methodType.returnType());

        LabelNode end = new LabelNode("end");

        Block equalsCall = new Block(context)
                .setDescription("invoke")
                .comment("equal");
        ArrayList<Class<?>> stackTypes = new ArrayList<>();
        for (int i = 0; i < functionBinding.getArguments().size(); i++) {
            equalsCall.append(functionBinding.getArguments().get(i));
            stackTypes.add(methodType.parameterType(i));
            equalsCall.append(ByteCodeUtils.ifWasNullPopAndGoto(context, end, unboxedReturnType, Lists.reverse(stackTypes)));
        }

        equalsCall.invokeDynamic(functionBinding.getName(), functionBinding.getCallSite().type(), functionBinding.getBindingId())
            .visitLabel(end);

        Block conditionBlock = new Block(context)
                .append(equalsCall)
                .append(ByteCodeUtils.ifWasNullClearPopAndGoto(context, notMatch, void.class, boolean.class));

        // if first and second are equal, return null
        Block trueBlock = new Block(context)
                .putVariable("wasNull", true)
                .pop(first.getType().getJavaType())
                .pushJavaDefault(first.getType().getJavaType());

        // else return first (which is still on the stack
        block.append(new IfStatement(context, conditionBlock, trueBlock, notMatch));

        return block;
    }

    private ByteCodeNode cast(ByteCodeGeneratorContext generatorContext, ByteCodeNode argument, Type fromType, Type toType)
    {
        FunctionInfo function = generatorContext
            .getRegistry()
            .getCoercion(fromType, toType);

        FunctionBinding binding = generatorContext
                .getBootstrapBinder()
                .bindFunction(function.getSignature().getName(), generatorContext.generateGetSession(), ImmutableList.of(argument), function.getFunctionBinder());

        return generateFunctionCall(function.getSignature(), generatorContext.getContext(), binding, "cast(" + fromType + ", " + toType + ")");
    }
}
