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
package com.facebook.presto.sql;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.planner.ExpressionInterpreter;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FunctionInvoker
{
    private final FunctionRegistry registry;

    public FunctionInvoker(FunctionRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
    }

    public Object invoke(Signature function, ConnectorSession session, Object... arguments)
    {
        return invoke(function, session, Arrays.asList(arguments));
    }

    /**
     * Arguments must be the native container type for the corresponding SQL types.
     *
     * Returns a value in the native container type corresponding to the declared SQL return type
     */
    public Object invoke(Signature function, ConnectorSession session, List<Object> arguments)
    {
        ScalarFunctionImplementation implementation = registry.getScalarFunctionImplementation(function);
        return ExpressionInterpreter.invoke(session, implementation, arguments);
    }
}
