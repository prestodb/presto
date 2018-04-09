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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.SqlTypeBytecodeExpression;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
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

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
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
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.divide;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.getStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.subtract;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;

public final class MapTransformKeyFunction
        extends SqlScalarFunction
{
    public static final MapTransformKeyFunction MAP_TRANSFORM_KEY_FUNCTION = new MapTransformKeyFunction();
    private static final MethodHandle STATE_FACTORY = methodHandle(MapTransformKeyFunction.class, "createState", MapType.class);

    private MapTransformKeyFunction()
    {
        super(new Signature(
                "transform_keys",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K1"), typeVariable("K2"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K2,V)"),
                ImmutableList.of(parseTypeSignature("map(K1,V)"), parseTypeSignature("function(K1,V,K2)")),
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
        return "apply lambda to each entry of the map and transform the key";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K1");
        Type transformedKeyType = boundVariables.getTypeVariable("K2");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType resultMapType = (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(transformedKeyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                generateTransformKey(keyType, transformedKeyType, valueType, resultMapType),
                Optional.of(STATE_FACTORY.bindTo(resultMapType)),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object createState(MapType mapType)
    {
        return new PageBuilder(ImmutableList.of(mapType));
    }

    private static MethodHandle generateTransformKey(Type keyType, Type transformedKeyType, Type valueType, Type resultMapType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());
        Class<?> transformedKeyJavaType = Primitives.wrap(transformedKeyType.getJavaType());
        Class<?> valueJavaType = Primitives.wrap(valueType.getJavaType());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MapTransformKey"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        Parameter state = arg("state", Object.class);
        Parameter session = arg("session", ConnectorSession.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(Block.class),
                ImmutableList.of(state, session, block, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable positionCount = scope.declareVariable(int.class, "positionCount");
        Variable position = scope.declareVariable(int.class, "position");
        Variable pageBuilder = scope.declareVariable(PageBuilder.class, "pageBuilder");
        Variable mapBlockBuilder = scope.declareVariable(BlockBuilder.class, "mapBlockBuilder");
        Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder");
        Variable typedSet = scope.declareVariable(TypedSet.class, "typeSet");
        Variable keyElement = scope.declareVariable(keyJavaType, "keyElement");
        Variable transformedKeyElement = scope.declareVariable(transformedKeyJavaType, "transformedKeyElement");
        Variable valueElement = scope.declareVariable(valueJavaType, "valueElement");

        // invoke block.getPositionCount()
        body.append(positionCount.set(block.invoke("getPositionCount", int.class)));

        // prepare the single map block builder
        body.append(pageBuilder.set(state.cast(PageBuilder.class)));
        body.append(new IfStatement()
                .condition(pageBuilder.invoke("isFull", boolean.class))
                .ifTrue(pageBuilder.invoke("reset", void.class)));
        body.append(mapBlockBuilder.set(pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(0))));
        body.append(blockBuilder.set(mapBlockBuilder.invoke("beginBlockEntry", BlockBuilder.class)));

        // create typed set
        body.append(typedSet.set(newInstance(
                TypedSet.class,
                constantType(binder, transformedKeyType),
                divide(positionCount, constantInt(2)),
                constantString(MAP_TRANSFORM_KEY_FUNCTION.getSignature().getName()))));

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
            // make sure invokeExact will not take uninitialized keys during compile time
            loadValueElement = new BytecodeBlock().append(valueElement.set(constantNull(valueJavaType)));
        }

        SqlTypeBytecodeExpression transformedKeySqlType = constantType(binder, transformedKeyType);
        BytecodeNode writeKeyElement;
        BytecodeNode throwDuplicatedKeyException;
        if (!transformedKeyType.equals(UNKNOWN)) {
            writeKeyElement = new BytecodeBlock()
                    .append(transformedKeyElement.set(function.invoke("apply", Object.class, keyElement.cast(Object.class), valueElement.cast(Object.class)).cast(transformedKeyJavaType)))
                    .append(new IfStatement()
                            .condition(equal(transformedKeyElement, constantNull(transformedKeyJavaType)))
                            .ifTrue(throwNullKeyException)
                            .ifFalse(new BytecodeBlock()
                                    .append(constantType(binder, transformedKeyType).writeValue(blockBuilder, transformedKeyElement.cast(transformedKeyType.getJavaType())))
                                    .append(valueSqlType.invoke("appendTo", void.class, block, add(position, constantInt(1)), blockBuilder))));

            // make sure getObjectValue takes a known key type
            throwDuplicatedKeyException = new BytecodeBlock()
                    .append(mapBlockBuilder.invoke("closeEntry", BlockBuilder.class).pop())
                    .append(newInstance(
                            PrestoException.class,
                            getStatic(INVALID_FUNCTION_ARGUMENT.getDeclaringClass(), "INVALID_FUNCTION_ARGUMENT").cast(ErrorCodeSupplier.class),
                            invokeStatic(
                                    String.class,
                                    "format",
                                    String.class,
                                    constantString("Duplicate keys (%s) are not allowed"),
                                    newArray(type(Object[].class), ImmutableList.of(transformedKeySqlType.invoke("getObjectValue", Object.class, session, blockBuilder.cast(Block.class), position))))))
                    .throwObject();
        }
        else {
            // key cannot be unknown
            // if we reach this point during runtime, it is an exception
            writeKeyElement = throwNullKeyException;
            throwDuplicatedKeyException = throwNullKeyException;
        }

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(incrementVariable(position, (byte) 2))
                .body(new BytecodeBlock()
                        .append(loadKeyElement)
                        .append(loadValueElement)
                        .append(writeKeyElement)
                        .append(new IfStatement()
                                .condition(typedSet.invoke("contains", boolean.class, blockBuilder.cast(Block.class), position))
                                .ifTrue(throwDuplicatedKeyException)
                                .ifFalse(typedSet.invoke("add", void.class, blockBuilder.cast(Block.class), position)))));

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

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), MapTransformKeyFunction.class.getClassLoader());
        return methodHandle(generatedClass, "transform", Object.class, ConnectorSession.class, Block.class, BinaryFunctionInterface.class);
    }
}
