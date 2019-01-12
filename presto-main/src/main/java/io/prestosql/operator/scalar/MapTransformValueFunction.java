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
package io.prestosql.operator.scalar;

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
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.gen.CallSiteBinder;
import io.prestosql.sql.gen.SqlTypeBytecodeExpression;
import io.prestosql.sql.gen.lambda.BinaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.getStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.subtract;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Reflection.methodHandle;

public final class MapTransformValueFunction
        extends SqlScalarFunction
{
    public static final MapTransformValueFunction MAP_TRANSFORM_VALUE_FUNCTION = new MapTransformValueFunction();
    private static final MethodHandle STATE_FACTORY = methodHandle(MapTransformKeyFunction.class, "createState", MapType.class);

    private MapTransformValueFunction()
    {
        super(new Signature(
                "transform_values",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V1"), typeVariable("V2")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V2)"),
                ImmutableList.of(parseTypeSignature("map(K,V1)"), parseTypeSignature("function(K,V1,V2)")),
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
        return "apply lambda to each entry of the map and transform the value";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V1");
        Type transformedValueType = boundVariables.getTypeVariable("V2");
        Type resultMapType = typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(transformedValueType.getTypeSignature())));
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                generateTransform(keyType, valueType, transformedValueType, resultMapType),
                Optional.of(STATE_FACTORY.bindTo(resultMapType)),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object createState(MapType mapType)
    {
        return new PageBuilder(ImmutableList.of(mapType));
    }

    private static MethodHandle generateTransform(Type keyType, Type valueType, Type transformedValueType, Type resultMapType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());
        Class<?> valueJavaType = Primitives.wrap(valueType.getJavaType());
        Class<?> transformedValueJavaType = Primitives.wrap(transformedValueType.getJavaType());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MapTransformValue"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        // define transform method
        Parameter state = arg("state", Object.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(Block.class),
                ImmutableList.of(state, block, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable positionCount = scope.declareVariable(int.class, "positionCount");
        Variable position = scope.declareVariable(int.class, "position");
        Variable pageBuilder = scope.declareVariable(PageBuilder.class, "pageBuilder");
        Variable mapBlockBuilder = scope.declareVariable(BlockBuilder.class, "mapBlockBuilder");
        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        Variable keyElement = scope.declareVariable(keyJavaType, "keyElement");
        Variable valueElement = scope.declareVariable(valueJavaType, "valueElement");
        Variable transformedValueElement = scope.declareVariable(transformedValueJavaType, "transformedValueElement");

        // invoke block.getPositionCount()
        body.append(positionCount.set(block.invoke("getPositionCount", int.class)));

        // prepare the single map block builder
        body.append(pageBuilder.set(state.cast(PageBuilder.class)));
        body.append(new IfStatement()
                .condition(pageBuilder.invoke("isFull", boolean.class))
                .ifTrue(pageBuilder.invoke("reset", void.class)));
        body.append(mapBlockBuilder.set(pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(0))));
        body.append(blockBuilder.set(mapBlockBuilder.invoke("beginBlockEntry", BlockBuilder.class)));

        // throw null key exception block
        BytecodeNode throwNullKeyException = new BytecodeBlock()
                .append(newInstance(
                        PrestoException.class,
                        getStatic(INVALID_FUNCTION_ARGUMENT.getDeclaringClass(), "INVALID_FUNCTION_ARGUMENT").cast(ErrorCodeSupplier.class),
                        constantString("map key cannot be null")))
                .throwObject();

        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);
        BytecodeNode loadKeyElement;
        if (!keyType.equals(UNKNOWN)) {
            loadKeyElement = new BytecodeBlock().append(keyElement.set(keySqlType.getValue(block, position).cast(keyJavaType)));
        }
        else {
            // make sure invokeExact will not take uninitialized keys during compile time
            // but if we reach this point during runtime, it is an exception
            // also close the block builder before throwing as we may be in a TRY() call
            // so that subsequent calls do not find it in an inconsistent state
            loadKeyElement = new BytecodeBlock()
                    .append(mapBlockBuilder.invoke("closeEntry", BlockBuilder.class).pop())
                    .append(keyElement.set(constantNull(keyJavaType)))
                    .append(throwNullKeyException);
        }

        SqlTypeBytecodeExpression valueSqlType = constantType(binder, valueType);
        BytecodeNode loadValueElement;
        if (!valueType.equals(UNKNOWN)) {
            loadValueElement = new IfStatement()
                    .condition(block.invoke("isNull", boolean.class, add(position, constantInt(1))))
                    .ifTrue(valueElement.set(constantNull(valueJavaType)))
                    .ifFalse(valueElement.set(valueSqlType.getValue(block, add(position, constantInt(1))).cast(valueJavaType)));
        }
        else {
            loadValueElement = new BytecodeBlock().append(valueElement.set(constantNull(valueJavaType)));
        }

        BytecodeNode writeTransformedValueElement;
        if (!transformedValueType.equals(UNKNOWN)) {
            writeTransformedValueElement = new IfStatement()
                    .condition(equal(transformedValueElement, constantNull(transformedValueJavaType)))
                    .ifTrue(blockBuilder.invoke("appendNull", BlockBuilder.class).pop())
                    .ifFalse(constantType(binder, transformedValueType).writeValue(blockBuilder, transformedValueElement.cast(transformedValueType.getJavaType())));
        }
        else {
            writeTransformedValueElement = new BytecodeBlock().append(blockBuilder.invoke("appendNull", BlockBuilder.class).pop());
        }

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(incrementVariable(position, (byte) 2))
                .body(new BytecodeBlock()
                        .append(loadKeyElement)
                        .append(loadValueElement)
                        .append(transformedValueElement.set(function.invoke("apply", Object.class, keyElement.cast(Object.class), valueElement.cast(Object.class)).cast(transformedValueJavaType)))
                        .append(keySqlType.invoke("appendTo", void.class, block, position, blockBuilder))
                        .append(writeTransformedValueElement)));

        body.append(mapBlockBuilder
                .invoke("closeEntry", BlockBuilder.class)
                .pop());
        body.append(pageBuilder.invoke("declarePosition", void.class));
        body.append(constantType(binder, resultMapType)
                .invoke(
                        "getObject",
                        Object.class,
                        mapBlockBuilder.cast(Block.class),
                        subtract(mapBlockBuilder.invoke("getPositionCount", int.class), constantInt(1)))
                .ret());

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), MapTransformValueFunction.class.getClassLoader());
        return methodHandle(generatedClass, "transform", Object.class, Block.class, BinaryFunctionInterface.class);
    }
}
