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
package com.facebook.presto.spi.procedure;

import com.facebook.presto.spi.ConnectorSession;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalProcedure
        extends Procedure
{
    private final MethodHandle methodHandle;

    public LocalProcedure(String schema, String name, List<Argument> arguments)
    {
        super(schema, name, arguments);
        this.methodHandle = null;
    }

    public LocalProcedure(String schema, String name, List<Argument> arguments, MethodHandle methodHandle)
    {
        super(schema, name, arguments);
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");

        checkArgument(!methodHandle.isVarargsCollector(), "Method must have fixed arity");
        checkArgument(methodHandle.type().returnType() == void.class, "Method must return void");

        long parameterCount = methodHandle.type().parameterList().stream()
                .filter(type -> !ConnectorSession.class.isAssignableFrom(type))
                .count();
        checkArgument(parameterCount == arguments.size(), "Method parameter count must match arguments");
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }
}
