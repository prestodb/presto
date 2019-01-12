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

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.Variable;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.a;
import static io.prestosql.sql.gen.BytecodeUtils.invoke;
import static java.util.Objects.requireNonNull;

public final class CachedInstanceBinder
{
    private final ClassDefinition classDefinition;
    private final CallSiteBinder callSiteBinder;
    private final Map<FieldDefinition, MethodHandle> initializers = new HashMap<>();
    private int nextId;

    public CachedInstanceBinder(ClassDefinition classDefinition, CallSiteBinder callSiteBinder)
    {
        this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
        this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
    }

    public CallSiteBinder getCallSiteBinder()
    {
        return callSiteBinder;
    }

    public FieldDefinition getCachedInstance(MethodHandle methodHandle)
    {
        FieldDefinition field = classDefinition.declareField(a(PRIVATE, FINAL), "__cachedInstance" + nextId, methodHandle.type().returnType());
        initializers.put(field, methodHandle);
        nextId++;
        return field;
    }

    public void generateInitializations(Variable thisVariable, BytecodeBlock block)
    {
        for (Map.Entry<FieldDefinition, MethodHandle> entry : initializers.entrySet()) {
            Binding binding = callSiteBinder.bind(entry.getValue());
            block.append(thisVariable)
                    .append(invoke(binding, "instanceFieldConstructor"))
                    .putField(entry.getKey());
        }
    }
}
