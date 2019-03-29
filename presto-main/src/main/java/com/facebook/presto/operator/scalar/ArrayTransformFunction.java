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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.lambda.UnaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.control.IfStatement;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.subtract;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;

public final class ArrayTransformFunction
        extends SqlScalarFunction
{
    public static final ArrayTransformFunction ARRAY_TRANSFORM_FUNCTION = new ArrayTransformFunction();

    private ArrayTransformFunction()
    {
        super(new Signature(
                "transform",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T"), typeVariable("U")),
                ImmutableList.of(),
                parseTypeSignature("array(U)"),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature("function(T,U)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "apply lambda to each element of the array";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        Type outputType = boundVariables.getTypeVariable("U");
        Class<?> generatedClass = generateTransform(inputType, outputType);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(UnaryFunctionInterface.class)),
                methodHandle(generatedClass, "transform", PageBuilder.class, Block.class, UnaryFunctionInterface.class),
                Optional.of(methodHandle(generatedClass, "createPageBuilder")));
    }

    private static Class<?> generateTransform(Type inputType, Type outputType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        Class<?> inputJavaType = Primitives.wrap(inputType.getJavaType());
        Class<?> outputJavaType = Primitives.wrap(outputType.getJavaType());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("ArrayTransform"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        // define createPageBuilder
        MethodDefinition createPageBuilderMethod = definition.declareMethod(a(PUBLIC, STATIC), "createPageBuilder", type(PageBuilder.class));
        createPageBuilderMethod.getBody()
                .append(newInstance(PageBuilder.class, constantType(binder, new ArrayType(outputType)).invoke("getTypeParameters", List.class)).ret());

        // define transform method
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", UnaryFunctionInterface.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(Block.class),
                ImmutableList.of(pageBuilder, block, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable positionCount = scope.declareVariable(int.class, "positionCount");
        Variable position = scope.declareVariable(int.class, "position");
        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        Variable inputElement = scope.declareVariable(inputJavaType, "inputElement");
        Variable outputElement = scope.declareVariable(outputJavaType, "outputElement");

        // invoke block.getPositionCount()
        body.append(positionCount.set(block.invoke("getPositionCount", int.class)));

        // reset page builder if it is full
        body.append(new IfStatement()
                .condition(pageBuilder.invoke("isFull", boolean.class))
                .ifTrue(pageBuilder.invoke("reset", void.class)));

        // get block builder
        body.append(blockBuilder.set(pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(0))));

        BytecodeNode loadInputElement;
        if (!inputType.equals(UNKNOWN)) {
            loadInputElement = new IfStatement()
                    .condition(block.invoke("isNull", boolean.class, position))
                    .ifTrue(inputElement.set(constantNull(inputJavaType)))
                    .ifFalse(inputElement.set(constantType(binder, inputType).getValue(block, position).cast(inputJavaType)));
        }
        else {
            loadInputElement = new BytecodeBlock().append(inputElement.set(constantNull(inputJavaType)));
        }

        BytecodeNode writeOutputElement;
        if (!outputType.equals(UNKNOWN)) {
            writeOutputElement = new IfStatement()
                    .condition(equal(outputElement, constantNull(outputJavaType)))
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, outputType).writeValue(blockBuilder, outputElement.cast(outputType.getJavaType())));
        }
        else {
            writeOutputElement = new BytecodeBlock().append(blockBuilder.invoke("appendNull", BlockBuilder.class).pop());
        }

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(incrementVariable(position, (byte) 1))
                .body(new BytecodeBlock()
                        .append(loadInputElement)
                        .append(outputElement.set(function.invoke("apply", Object.class, inputElement.cast(Object.class)).cast(outputJavaType)))
                        .append(writeOutputElement)));

        body.append(pageBuilder.invoke("declarePositions", void.class, positionCount));

        body.append(blockBuilder.invoke("getRegion", Block.class, subtract(blockBuilder.invoke("getPositionCount", int.class), positionCount), positionCount).ret());

        return defineClass(definition, Object.class, binder.getBindings(), ArrayTransformFunction.class.getClassLoader());
    }
}
