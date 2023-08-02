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
package com.facebook.presto.operator.annotations;

import com.facebook.presto.spi.function.FunctionInstance;
import com.facebook.presto.spi.function.SqlFunction;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.annotations.FunctionsParserHelper.findPublicStaticFieldsOfType;

public class FunctionInstanceParser
{
    private FunctionInstanceParser()
    {
    }

    public static List<? extends SqlFunction> parseFunctionsFromAnnotation(Class<?> clazz)
    {
        return findPublicStaticFieldsOfType(clazz, SqlFunction.class, FunctionInstance.class)
                .stream()
                .map(x -> {
                    try {
                        return (SqlFunction) x.get(null);
                    }
                    catch (IllegalAccessException | ClassCastException e) {
                        // All fields should be of the correct type
                        throw new RuntimeException("failed to get function instance", e);
                    }
                }).collect(Collectors.toList());
    }
}
