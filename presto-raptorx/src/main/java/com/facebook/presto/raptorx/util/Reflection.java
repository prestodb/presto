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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.spi.PrestoException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public final class Reflection
{
    private Reflection() {}

    public static MethodHandle getMethod(Class<?> clazz, String name)
    {
        List<Method> methods = Arrays.stream(clazz.getMethods())
                .filter(method -> method.getName().equals(name))
                .collect(toList());
        checkArgument(methods.size() == 1, "expected one method [%s] [%s]", name, clazz.getName());
        return methodHandle(methods.get(0));
    }

    private static MethodHandle methodHandle(Method method)
    {
        try {
            return MethodHandles.lookup().unreflect(method);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(RAPTOR_INTERNAL_ERROR, e);
        }
    }
}
