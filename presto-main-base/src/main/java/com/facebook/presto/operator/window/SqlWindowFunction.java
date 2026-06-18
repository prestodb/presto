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
package com.facebook.presto.operator.window;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.BuiltInFunction;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;

import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static java.util.Objects.requireNonNull;

public class SqlWindowFunction
        extends BuiltInFunction
{
    private final WindowFunctionSupplier supplier;

    public SqlWindowFunction(WindowFunctionSupplier supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
    }

    @Override
    public final Signature getSignature()
    {
        return supplier.getSignature();
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return supplier.getDescription();
    }

    public WindowFunctionSupplier specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return supplier;
    }
}
