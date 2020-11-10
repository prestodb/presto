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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.ValueWindowFunction;
import com.facebook.presto.spi.function.WindowFunction;
import com.google.common.collect.Lists;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.util.List;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.spi.function.FunctionKind.WINDOW;
import static java.util.Objects.requireNonNull;

public class ReflectionWindowFunctionSupplier<T extends WindowFunction>
        extends AbstractWindowFunctionSupplier
{
    private final Constructor<T> constructor;

    public ReflectionWindowFunctionSupplier(String name, Type returnType, List<? extends Type> argumentTypes, Class<T> type)
    {
        this(new Signature(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, name), WINDOW, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature)), type);
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
    protected T newWindowFunction(List<Integer> inputs, boolean ignoreNulls)
    {
        try {
            T windowFunction;

            if (getSignature().getArgumentTypes().isEmpty()) {
                windowFunction = constructor.newInstance();
            }
            else {
                windowFunction = constructor.newInstance(inputs);
            }

            if (windowFunction instanceof ValueWindowFunction) {
                ((ValueWindowFunction) windowFunction).setIgnoreNulls(ignoreNulls);
            }

            return windowFunction;
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
