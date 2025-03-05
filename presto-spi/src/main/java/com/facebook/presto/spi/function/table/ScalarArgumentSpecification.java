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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.common.type.Type;

import static com.facebook.presto.spi.function.table.ConnectorTableFunction.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ScalarArgumentSpecification
        extends ArgumentSpecification
{
    private final Type type;

    public ScalarArgumentSpecification(String name, Type type)
    {
        super(name, true, null);
        this.type = requireNonNull(type, "type is null");
    }

    public ScalarArgumentSpecification(String name, Type type, Object defaultValue)
    {
        super(name, false, defaultValue);
        this.type = requireNonNull(type, "type is null");
        checkArgument(type.getJavaType().equals(defaultValue.getClass()), format("default value %s does not match the declared type: %s", defaultValue, type));
    }

    public Type getType()
    {
        return type;
    }
}
