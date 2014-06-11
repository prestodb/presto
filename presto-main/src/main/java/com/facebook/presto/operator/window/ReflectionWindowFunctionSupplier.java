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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ReflectionWindowFunctionSupplier<T extends WindowFunction>
        extends AbstractWindowFunctionSupplier
{
    private final Constructor<T> constructor;

    public ReflectionWindowFunctionSupplier(String name, Type returnType, List<? extends Type> argumentTypes, Class<T> type)
    {
        this(new Signature(name, returnType, argumentTypes, false), type);
    }

    public ReflectionWindowFunctionSupplier(Signature signature, Class<T> type)
    {
        super(signature, getDescription(checkNotNull(type, "type is null")));
        try {
            if (signature.getArgumentTypes().isEmpty()) {
                constructor = type.getConstructor();
            }
            else {
                constructor = type.getConstructor(List.class);
            }
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
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
        catch (InvocationTargetException e) {
            throw Throwables.propagate(e.getCause());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static String getDescription(AnnotatedElement annotatedElement)
    {
        Description description = annotatedElement.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }
}
