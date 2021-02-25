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
package com.facebook.presto.common.type.semantic;

import com.facebook.presto.common.type.Type;

import static com.facebook.presto.common.type.semantic.SemanticType.SemanticTypeCategory.BUILTIN_TYPE;
import static java.util.Objects.requireNonNull;

public class BuiltInType
        extends SemanticType
{
    private final String name;

    public BuiltInType(Type type)
    {
        super(BUILTIN_TYPE, requireNonNull(type, "type is null").getTypeSignature(), type);
        this.name = type.getDisplayName();
    }

    @Override
    public String getName()
    {
        return name;
    }
}
