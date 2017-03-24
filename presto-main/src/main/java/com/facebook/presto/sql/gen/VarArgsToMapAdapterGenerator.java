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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;

public class VarArgsToMapAdapterGenerator
{
    private VarArgsToMapAdapterGenerator()
    {
    }

    /**
     * Generate byte code that
     * <p><ul>
     * <li>takes a specified number of variables as arguments (types of the arguments are provided in {@code javaTypes})
     * <li>put the variables in a map (keys of the map are provided in {@code names})
     * <li>invoke the provided {@code function} with the map
     * <li>return with the result of the function call (type must match {@code returnType})
     * </ul></p>
     */
    public static MethodHandle generateVarArgsToMapAdapter(Class<?> returnType, List<Class<?>> javaTypes, List<String> names, Function<Map<String, Object>, Object> function)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(a(PUBLIC, FINAL), makeClassName("VarArgsToMapAdapter"), type(Object.class));

        ImmutableList.Builder<Parameter> parameterListBuilder = ImmutableList.builder();
        for (int i = 0; i < javaTypes.size(); i++) {
            Class<?> javaType = javaTypes.get(i);
            parameterListBuilder.add(arg("input_" + i, javaType));
        }
        ImmutableList<Parameter> parameterList = parameterListBuilder.build();

        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "varArgsToMap", ParameterizedType.type(returnType), parameterList);
        BytecodeBlock body = methodDefinition.getBody();

        // ImmutableMap.Builder can not be used here because it doesn't allow nulls.
        Variable map = methodDefinition.getScope().declareVariable(HashMap.class, "map");
        body.append(map.set(invokeStatic(Maps.class, "newHashMapWithExpectedSize", HashMap.class, constantInt(javaTypes.size()))));
        for (int i = 0; i < javaTypes.size(); i++) {
            body.append(map.invoke("put", Object.class, constantString(names.get(i)).cast(Object.class), parameterList.get(i).cast(Object.class)));
        }
        body.append(
                loadConstant(callSiteBinder, function, Function.class)
                        .invoke("apply", Object.class, map.cast(Object.class))
                        .cast(returnType)
                        .ret());

        Class<?> generatedClass = defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), new DynamicClassLoader(VarArgsToMapAdapterGenerator.class.getClassLoader()));
        return Reflection.methodHandle(generatedClass, "varArgsToMap", javaTypes.toArray(new Class<?>[javaTypes.size()]));
    }
}
