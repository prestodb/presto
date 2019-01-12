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
package io.prestosql.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Variable;
import io.prestosql.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.gen.BytecodeUtils.loadConstant;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;

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
        checkCondition(javaTypes.size() <= 254, NOT_SUPPORTED, "Too many arguments for vararg function");
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(a(PUBLIC, FINAL), makeClassName("VarArgsToMapAdapter"), type(Object.class));

        ImmutableList.Builder<Parameter> parameterListBuilder = ImmutableList.builder();
        for (int i = 0; i < javaTypes.size(); i++) {
            Class<?> javaType = javaTypes.get(i);
            parameterListBuilder.add(arg("input_" + i, javaType));
        }
        ImmutableList<Parameter> parameterList = parameterListBuilder.build();

        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "varArgsToMap", type(returnType), parameterList);
        BytecodeBlock body = methodDefinition.getBody();

        // ImmutableMap.Builder can not be used here because it doesn't allow nulls.
        Variable map = methodDefinition.getScope().declareVariable(
                "map",
                methodDefinition.getBody(),
                invokeStatic(Maps.class, "newHashMapWithExpectedSize", HashMap.class, constantInt(javaTypes.size())));
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
