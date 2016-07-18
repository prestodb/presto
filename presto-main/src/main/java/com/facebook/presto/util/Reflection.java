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
package com.facebook.presto.util;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class Reflection
{
    private Reflection() {}

    public static Field field(Class<?> clazz, String name)
    {
        try {
            return clazz.getField(name);
        }
        catch (NoSuchFieldException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public static Method method(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return clazz.getMethod(name, parameterTypes);
        }
        catch (NoSuchMethodException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public static MethodHandle methodHandle(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflect(clazz.getMethod(name, parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public static MethodHandle methodHandle(Method method)
    {
        try {
            return MethodHandles.lookup().unreflect(method);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public static MethodHandle constructorMethodHandle(StandardErrorCode errorCode, Class<?> clazz, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflectConstructor(clazz.getConstructor(parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(errorCode, e);
        }
    }
}
