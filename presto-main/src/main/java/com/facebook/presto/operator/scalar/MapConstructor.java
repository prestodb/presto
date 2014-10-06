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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.ByteCodeUtils;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
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
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.invoke.MethodHandles.lookup;

public final class MapConstructor
        extends ParametricScalar
{
    private final Signature signature;

    private final TypeManager typeManager;

    public MapConstructor(int pairs, TypeManager typeManager)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (int i = 0; i < pairs; i++) {
            builder.add("K");
            builder.add("V");
        }
        signature = new Signature("map", ImmutableList.of(typeParameter("K"), typeParameter("V")), "map<K,V>", builder.build(), false, true);
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Map constructor";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager)
    {
        Type keyType = types.get("K");
        Type valueType = types.get("V");

        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        ImmutableList.Builder<String> actualArgumentNames = ImmutableList.builder();
        for (int i = 0; i < arity; i++) {
            Type type;
            if (i % 2 == 0) {
                type = keyType;
            }
            else {
                type = valueType;
            }
            actualArgumentNames.add(type.getName());
            if (type.getJavaType().isPrimitive()) {
                builder.add(Primitives.wrap(type.getJavaType()));
            }
            else {
                builder.add(type.getJavaType());
            }
        }
        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateMapConstructor(stackTypes, valueType);
        MethodHandle methodHandle;
        try {
            Method method = clazz.getMethod("mapConstructor", stackTypes.toArray(new Class<?>[stackTypes.size()]));
            methodHandle = lookup().unreflect(method);
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        Type mapType = this.typeManager.getParameterizedType(MAP.getName(), ImmutableList.of(keyType.getName(), valueType.getName()));
        Signature signature = new Signature("map", ImmutableList.<TypeParameter>of(), mapType.getName(), actualArgumentNames.build(), false, true);
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), true));
        return new FunctionInfo(signature, "Constructs a map of the given entries", true, methodHandle, true, false, nullableParameters);
    }

    private static Class<?> generateMapConstructor(List<Class<?>> stackTypes, Type valueType)
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
                makeClassName(Joiner.on("").join(stackTypeNames) + "MapConstructor"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate mapConstructor()
        ImmutableList.Builder<NamedParameterDefinition> parameters = ImmutableList.builder();
        for (int i = 0; i < stackTypes.size(); i++) {
            Class<?> stackType = stackTypes.get(i);
            parameters.add(arg("arg" + i, stackType));
        }

        CompilerContext context = new CompilerContext(null);
        Block body = definition.declareMethod(context, a(PUBLIC, STATIC), "mapConstructor", type(Slice.class), parameters.build())
                .getBody();

        Variable valuesVariable = context.declareVariable(Map.class, "values");
        body.comment("Map<Object, Object> values = new LinkedHashMap();")
                .newObject(LinkedHashMap.class)
                .dup()
                .invokeConstructor(LinkedHashMap.class)
                .putVariable(valuesVariable);

        for (int i = 0; i < stackTypes.size(); i += 2) {
            body.comment("values.put(arg%d, arg%d)", i, i + 1)
                    .getVariable(valuesVariable)
                    .getVariable("arg" + i);
            Class<?> stackType = stackTypes.get(i);
            if (stackType.isPrimitive()) {
                body.append(ByteCodeUtils.boxPrimitiveIfNecessary(context, stackType));
            }
            body.getVariable("arg" + (i + 1));
            stackType = stackTypes.get(i + 1);
            if (stackType.isPrimitive()) {
                body.append(ByteCodeUtils.boxPrimitiveIfNecessary(context, stackType));
            }
            body.invokeInterface(Map.class, "put", Object.class, Object.class, Object.class);
        }

        if (valueType instanceof ArrayType || valueType instanceof MapType) {
            body.comment("return rawValueSlicesToStackRepresentation(values);")
                    .getVariable(valuesVariable)
                    .invokeStatic(MapType.class, "rawValueSlicesToStackRepresentation", Slice.class, Map.class)
                    .retObject();
        }
        else {
            body.comment("return toStackRepresentation(values);")
                    .getVariable(valuesVariable)
                    .invokeStatic(MapType.class, "toStackRepresentation", Slice.class, Map.class)
                    .retObject();
        }

        return defineClass(definition, Object.class, new DynamicClassLoader());
    }
}
