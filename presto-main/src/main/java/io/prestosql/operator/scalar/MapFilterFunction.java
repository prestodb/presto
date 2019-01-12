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
import io.prestosql.spi.PageBuilder;
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
import static io.airlift.bytecode.expression.BytecodeExpressions.and;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.airlift.bytecode.expression.BytecodeExpressions.notEqual;
import static io.airlift.bytecode.expression.BytecodeExpressions.subtract;
import static io.airlift.bytecode.instruction.VariableInstruction.incrementVariable;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.functionTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Reflection.methodHandle;

public final class MapFilterFunction
        extends SqlScalarFunction
{
    public static final MapFilterFunction MAP_FILTER_FUNCTION = new MapFilterFunction();
    private static final MethodHandle STATE_FACTORY = methodHandle(MapFilterFunction.class, "createState", MapType.class);

    private MapFilterFunction()
    {
        super(new Signature(
                "map_filter",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("map(K,V)"),
                ImmutableList.of(parseTypeSignature("map(K,V)"), parseTypeSignature("function(K,V,boolean)")),
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
        return "return map containing entries that match the given predicate";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");
        MapType mapType = (MapType) typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        functionTypeArgumentProperty(BinaryFunctionInterface.class)),
                generateFilter(mapType),
                Optional.of(STATE_FACTORY.bindTo(mapType)),
                isDeterministic());
    }

    @UsedByGeneratedCode
    public static Object createState(MapType mapType)
    {
        return new PageBuilder(ImmutableList.of(mapType));
    }

    private static MethodHandle generateFilter(MapType mapType)
    {
        CallSiteBinder binder = new CallSiteBinder();
        Type keyType = mapType.getKeyType();
        Type valueType = mapType.getValueType();
        Class<?> keyJavaType = Primitives.wrap(keyType.getJavaType());
        Class<?> valueJavaType = Primitives.wrap(valueType.getJavaType());

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("MapFilter"),
                type(Object.class));
        definition.declareDefaultConstructor(a(PRIVATE));

        Parameter state = arg("state", Object.class);
        Parameter block = arg("block", Block.class);
        Parameter function = arg("function", BinaryFunctionInterface.class);
        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "filter",
                type(Block.class),
                ImmutableList.of(state, block, function));

        BytecodeBlock body = method.getBody();
        Scope scope = method.getScope();
        Variable positionCount = scope.declareVariable(int.class, "positionCount");
        Variable position = scope.declareVariable(int.class, "position");
        Variable pageBuilder = scope.declareVariable(PageBuilder.class, "pageBuilder");
        Variable mapBlockBuilder = scope.declareVariable(BlockBuilder.class, "mapBlockBuilder");
        Variable singleMapBlockWriter = scope.declareVariable(BlockBuilder.class, "singleMapBlockWriter");
        Variable keyElement = scope.declareVariable(keyJavaType, "keyElement");
        Variable valueElement = scope.declareVariable(valueJavaType, "valueElement");
        Variable keep = scope.declareVariable(Boolean.class, "keep");

        // invoke block.getPositionCount()
        body.append(positionCount.set(block.invoke("getPositionCount", int.class)));

        // prepare the single map block builder
        body.append(pageBuilder.set(state.cast(PageBuilder.class)));
        body.append(new IfStatement()
                .condition(pageBuilder.invoke("isFull", boolean.class))
                .ifTrue(pageBuilder.invoke("reset", void.class)));
        body.append(mapBlockBuilder.set(pageBuilder.invoke("getBlockBuilder", BlockBuilder.class, constantInt(0))));
        body.append(singleMapBlockWriter.set(mapBlockBuilder.invoke("beginBlockEntry", BlockBuilder.class)));

        SqlTypeBytecodeExpression keySqlType = constantType(binder, keyType);
        BytecodeNode loadKeyElement;
        if (!keyType.equals(UNKNOWN)) {
            // key element must be non-null
            loadKeyElement = new BytecodeBlock().append(keyElement.set(keySqlType.getValue(block, position).cast(keyJavaType)));
        }
        else {
            loadKeyElement = new BytecodeBlock().append(keyElement.set(constantNull(keyJavaType)));
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

        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(incrementVariable(position, (byte) 2))
                .body(new BytecodeBlock()
                        .append(loadKeyElement)
                        .append(loadValueElement)
                        .append(keep.set(function.invoke("apply", Object.class, keyElement.cast(Object.class), valueElement.cast(Object.class)).cast(Boolean.class)))
                        .append(new IfStatement("if (keep != null && keep) ...")
                                .condition(and(notEqual(keep, constantNull(Boolean.class)), keep.cast(boolean.class)))
                                .ifTrue(new BytecodeBlock()
                                        .append(keySqlType.invoke("appendTo", void.class, block, position, singleMapBlockWriter))
                                        .append(valueSqlType.invoke("appendTo", void.class, block, add(position, constantInt(1)), singleMapBlockWriter))))));

        body.append(mapBlockBuilder
                .invoke("closeEntry", BlockBuilder.class)
                .pop());
        body.append(pageBuilder.invoke("declarePosition", void.class));
        body.append(constantType(binder, mapType)
                .invoke(
                        "getObject",
                        Object.class,
                        mapBlockBuilder.cast(Block.class),
                        subtract(mapBlockBuilder.invoke("getPositionCount", int.class), constantInt(1)))
                .ret());

        Class<?> generatedClass = defineClass(definition, Object.class, binder.getBindings(), MapFilterFunction.class.getClassLoader());
        return methodHandle(generatedClass, "filter", Object.class, Block.class, BinaryFunctionInterface.class);
    }
}
