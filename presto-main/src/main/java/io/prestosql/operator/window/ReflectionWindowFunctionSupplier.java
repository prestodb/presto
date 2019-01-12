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
package io.prestosql.operator.window;

import com.google.common.collect.Lists;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.WindowFunction;
import io.prestosql.spi.type.Type;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.util.List;

import static io.prestosql.metadata.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

public class ReflectionWindowFunctionSupplier<T extends WindowFunction>
        extends AbstractWindowFunctionSupplier
{
    private final Constructor<T> constructor;

    public ReflectionWindowFunctionSupplier(String name, Type returnType, List<? extends Type> argumentTypes, Class<T> type)
    {
        this(new Signature(name, WINDOW, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature)), type);
    }

    public ReflectionWindowFunctionSupplier(Signature signature, Class<T> type)
    {
        super(signature, getDescription(requireNonNull(type, "type is null")));
        try {
            if (signature.getArgumentTypes().isEmpty()) {
                constructor = type.getConstructor();
            }
            else {
                constructor = type.getConstructor(List.class);
            }
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected T newWindowFunction(List<Integer> inputs)
    {
        try {
            if (getSignature().getArgumentTypes().isEmpty()) {
                return constructor.newInstance();
            }
            else {
                return constructor.newInstance(inputs);
            }
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getDescription(AnnotatedElement annotatedElement)
    {
        Description description = annotatedElement.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }
}
