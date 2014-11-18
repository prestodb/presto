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

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.CompilerContext;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.NamedParameterDefinition;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.byteCode.instruction.JumpInstruction;
import com.facebook.presto.byteCode.instruction.LabelNode;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.ByteCodeUtils;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.NamedParameterDefinition.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.sql.relational.Signatures.arrayConstructorSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;

public final class Least
        extends ParametricScalar
{
    public static final Least LEAST = new Least();
    private static final Signature SIGNATURE = new Signature("least", ImmutableList.of(typeParameter("E")), "E", ImmutableList.of("E"), true, true);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        // Internal function, doesn't need a description
        return "";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        checkArgument(types.size() == 1, "Can only construct arrays from exactly matching types");
        Class<?> res;
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        Type type = types.get("E");
        checkArgument(type.isComparable(), "Type should be comparable");
        for (int i = 0; i < arity; i++) {
            if (type.getJavaType().isPrimitive()) {
                builder.add(Primitives.wrap(type.getJavaType()));
            }
            else {
                builder.add(type.getJavaType());
            }
        }
        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateLeast(stackTypes, type);
        MethodHandle methodHandle;
        try {
            Method method = clazz.getMethod("least", stackTypes.toArray(new Class<?>[stackTypes.size()]));
            methodHandle = lookup().unreflect(method);
        }
        catch (ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), true));
        return new FunctionInfo(arrayConstructorSignature(type.getTypeSignature(), Collections.nCopies(arity, type.getTypeSignature())), "Finds the least element", true, methodHandle, true, false, nullableParameters);
    }

    private static Class<?> generateLeast(List<Class<?>> stackTypes, Type elementType)
    {
        List<String> stackTypeNames = FluentIterable.from(stackTypes).transform(new Function<Class<?>, String>() {
            @Override
            public String apply(Class<?> input)
            {
                return input.getSimpleName();
            }
        }).toList();
        ClassDefinition definition = new ClassDefinition(
                new CompilerContext(null),
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(stackTypeNames) + "Least"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            parameters.add(arg("arg" + i, stackType));
        }

        CompilerContext context = new CompilerContext(null);
        Block body = definition.declareMethod(context, a(PUBLIC, STATIC), "least", type(stackTypes.get(0)), parameters.build())
                .getBody();

        Variable ansVariable = context.declareVariable(Object.class, "ans");

        body.comment("Object ans = arg0;")
                .newObject(stackTypes.get(0))
                .getVariable("arg0")
                .putVariable(ansVariable);

        for (int i = 1; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            if (stackType.isPrimitive()) {
                body.append(ByteCodeUtils.boxPrimitiveIfNecessary(context, stackType));
            }
            LabelNode end = new LabelNode("end");
            Block cool = new Block(context)
                    .getVariable(ansVariable)
                    .getVariable("arg" + i)
                    .invokeVirtual(stackType, "compareTo", int.class, stackType)
                    .push(0)
                    .append(JumpInstruction.jumpIfIntLessThan(end))
                    .getVariable("arg" + i)
                    .putVariable(ansVariable)
                    .visitLabel(end);
            body.append(cool);
        }

        body.comment("return ans;")
                    .getVariable(ansVariable)
                    .retObject();

        return defineClass(definition, Object.class, new DynamicClassLoader());
    }
}
