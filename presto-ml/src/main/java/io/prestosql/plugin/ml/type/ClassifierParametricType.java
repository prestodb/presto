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
package io.prestosql.plugin.ml.type;

import io.prestosql.spi.type.ParameterKind;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ClassifierParametricType
        implements ParametricType
{
    public static final String NAME = "Classifier";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 1, "Expected only one type, got %s", parameters);
        checkArgument(
                parameters.get(0).getKind() == ParameterKind.TYPE,
                "Expected type as a parameter, got %s",
                parameters);
        return new ClassifierType(parameters.get(0).getType());
    }
}
