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
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.sql.gen.CachedInstanceBinder;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.USE_NULL_FLAG;
import static com.facebook.presto.spi.function.Signature.withVariadicBound;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.gen.InvokeFunctionBytecodeExpression.invokeFunction;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;

public class RowIndeterminateOperator
        extends SqlOperator
{
    public static final RowIndeterminateOperator ROW_INDETERMINATE = new RowIndeterminateOperator();

    private RowIndeterminateOperator()
    {
        super(INDETERMINATE, ImmutableList.of(withVariadicBound("T", "row")), ImmutableList.of(), BOOLEAN.getTypeSignature(), ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type type = boundVariables.getTypeVariable("T");
        Class<?> indeterminateOperatorClass = generateIndeterminate(type, functionAndTypeManager);
        MethodHandle indeterminateMethod = methodHandle(indeterminateOperatorClass, "indeterminate", type.getJavaType(), boolean.class);
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(USE_NULL_FLAG)),
                indeterminateMethod);
    }

    private static Class<?> generateIndeterminate(Type type, FunctionAndTypeManager functionAndTypeManager)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("RowIndeterminateOperator"),
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
        boolean hasUnknownFields = fieldTypes.stream().anyMatch(fieldType -> fieldType.equals(UNKNOWN));

        body.append(new IfStatement("if isNull is true...")
                .condition(isNull)
                .ifTrue(new BytecodeBlock()
                        .push(true)
                        .gotoLabel(end)));

        if (hasUnknownFields) {
            // if the current field type is UNKNOWN which means this field is null, directly return true
            body.push(true)
                    .gotoLabel(end);
        }
        else {
            for (int i = 0; i < fieldTypes.size(); i++) {
                IfStatement ifNullField = new IfStatement("if the field is null...");
                ifNullField.condition(value.invoke("isNull", boolean.class, constantInt(i)))
                        .ifTrue(new BytecodeBlock()
                                .push(true)
                                .gotoLabel(end));

                FunctionHandle functionHandle = functionAndTypeManager.resolveOperator(INDETERMINATE, fromTypes(fieldTypes.get(i)));

                BuiltInScalarFunctionImplementation function = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionHandle);
                BytecodeExpression element = constantType(binder, fieldTypes.get(i)).getValue(value, constantInt(i));

                ifNullField.ifFalse(new IfStatement("if the field is not null but indeterminate...")
                        .condition(invokeFunction(scope, cachedInstanceBinder, functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName(), function, element))
                        .ifTrue(new BytecodeBlock()
                                .push(true)
                                .gotoLabel(end)));

                body.append(ifNullField);
            }
            // if none of the fields is indeterminate, then push false
            body.push(false);
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
