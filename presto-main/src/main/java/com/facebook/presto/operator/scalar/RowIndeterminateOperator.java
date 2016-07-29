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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CachedInstanceBinder;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.InvokeFunctionBytecodeExpression.invokeFunction;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RowIndeterminateOperator
        extends SqlOperator
{
    public static final RowIndeterminateOperator ROW_INDETERMINATE = new RowIndeterminateOperator();

    private RowIndeterminateOperator()
    {
        super(INDETERMINATE, ImmutableList.of(withVariadicBound("T", "row")), ImmutableList.of(), BOOLEAN.getTypeSignature(), ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type type = boundVariables.getTypeVariable("T");
        Class<?> indeterminateOperatorClass = generateIndeterminate(type, functionRegistry);
        MethodHandle methodHandle = methodHandle(indeterminateOperatorClass, "indeterminate", type.getJavaType(), boolean.class);
        return new ScalarFunctionImplementation(false, ImmutableList.of(true), ImmutableList.of(true), methodHandle, isDeterministic());
    }

    private static Class<?> generateIndeterminate(Type type, FunctionRegistry functionRegistry)
    {
        CallSiteBinder binder = new CallSiteBinder();

        // Embed the MD5 hash code of input and output types into the generated class name instead of the raw type names
        // to avoid hitting class naming length or character set limits
        byte[] md5Suffix = Hashing.md5().hashBytes(type.toString().getBytes(UTF_8)).asBytes();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("$").join("RowIndeterminateOperator", BaseEncoding.base16().encode(md5Suffix))),
                type(Object.class));

        Parameter value = arg("value", type.getJavaType());
        Parameter isNull = arg("isNull", boolean.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "indeterminate",
                type(boolean.class),
                value,
                isNull);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantFalse()));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);

        LabelNode end = new LabelNode("end");
        List<Type> fieldTypes = type.getTypeParameters();
        boolean hasUnknownFields = fieldTypes.stream().anyMatch(t -> t.equals(UNKNOWN));

        body.append(new IfStatement("if isNull is true...")
                .condition(isNull)
                .ifTrue(new BytecodeBlock()
                        .push(true)
                        .append(wasNull.set(constantFalse()))
                        .gotoLabel(end)));

        if (hasUnknownFields) {
            // if the current field type is UNKNOWN which means this field is null, directly return true
            body.push(true)
                    .append(wasNull.set(constantFalse()))
                    .gotoLabel(end);
        }
        else {
            for (int i = 0; i < fieldTypes.size(); i++) {
                IfStatement ifNullField = new IfStatement("if the field is null...");
                ifNullField.condition(value.invoke("isNull", boolean.class, constantInt(i)))
                        .ifTrue(new BytecodeBlock()
                                .push(true)
                                .append(wasNull.set(constantFalse()))
                                .gotoLabel(end));

                // only invoke INDETERMINATE for the nested value when nested type is complex type
                Signature signature = internalOperator(
                        INDETERMINATE.name(),
                        BOOLEAN.getTypeSignature(),
                        ImmutableList.of(fieldTypes.get(i).getTypeSignature()));
                ScalarFunctionImplementation function = functionRegistry.getScalarFunctionImplementation(signature);
                BytecodeExpression element = constantType(binder, fieldTypes.get(i)).getValue(value, constantInt(i));

                ifNullField.ifFalse(new IfStatement("if the field is not null but indeterminate...")
                        .condition(invokeFunction(scope, cachedInstanceBinder, signature.getName(), function, element, constantFalse()))
                        .ifTrue(new BytecodeBlock()
                                .push(true)
                                .append(wasNull.set(constantFalse()))
                                .gotoLabel(end)));

                body.append(ifNullField);
            }
            // if none of the fields is indeterminate, then push false
            body.push(false)
                    .append(wasNull.set(constantFalse()));
        }

        body.visitLabel(end)
                .retBoolean();

        // create constructor
        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), RowIndeterminateOperator.class.getClassLoader());
    }
}
