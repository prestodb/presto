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
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.sql.gen.SqlTypeBytecodeExpression;
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.divide;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.equal;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.subtract;
import static com.facebook.presto.bytecode.instruction.VariableInstruction.incrementVariable;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class MapTransformKeyFunction
        extends SqlScalarFunction
{
    public static final MapTransformKeyFunction MAP_TRANSFORM_KEY_FUNCTION = new MapTransformKeyFunction();
    private static final MethodHandle STATE_FACTORY = methodHandle(MapTransformKeyFunction.class, "createState", MapType.class);

    private MapTransformKeyFunction()
    {
        super(new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "transform_keys"),
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K1"), typeVariable("K2"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K2,V)"),
                ImmutableList.of(parseTypeSignature("map(K1,V)"), parseTypeSignature("function(K1,V,K2)")),
                false));
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return SqlFunctionVisibility.PUBLIC;
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
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K1");
        Type transformedKeyType = boundVariables.getTypeVariable("K2");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType resultMapType = (MapType) functionAndTypeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(transformedKeyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                generateTransformKey(keyType, transformedKeyType, valueType, resultMapType),
                Optional.of(STATE_FACTORY.bindTo(resultMapType)));
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
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "transform",
                type(Block.class),
                ImmutableList.of(state, properties, block, function));

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
                constantString(MAP_TRANSFORM_KEY_FUNCTION.getSignature().getNameSuffix()))));

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
                                    newArray(type(Object[].class), ImmutableList.of(transformedKeySqlType.invoke("getObjectValue", Object.class, properties, blockBuilder.cast(Block.class), position))))))
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
                                .ifFalse(typedSet.invoke("add", boolean.class, blockBuilder.cast(Block.class), position)
                                .pop()))));

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
        return methodHandle(generatedClass, "transform", Object.class, SqlFunctionProperties.class, Block.class, BinaryFunctionInterface.class);
    }
}
