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
package io.prestosql.operator.annotations;

import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public final class LiteralImplementationDependency
        implements ImplementationDependency
{
    private final String literalName;

    public LiteralImplementationDependency(String literalName)
    {
        this.literalName = requireNonNull(literalName, "literalName is null");
    }

    @Override
    public Long resolve(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return boundVariables.getLongVariable(literalName);
    }
}
