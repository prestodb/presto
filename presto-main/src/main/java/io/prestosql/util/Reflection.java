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
package io.prestosql.util;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

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

    /**
     * Returns a MethodHandle corresponding to the specified method.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a method involves creating
     * JNI global weak references. G1 processes such references serially. As a result, calling this
     * method in a tight loop can create significant GC pressure and significantly increase
     * application pause time.
     */
    public static MethodHandle methodHandle(Class<?> clazz, String name, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflect(clazz.getMethod(name, parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    /**
     * Returns a MethodHandle corresponding to the specified method.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a method involves creating
     * JNI global weak references. G1 processes such references serially. As a result, calling this
     * method in a tight loop can create significant GC pressure and significantly increase
     * application pause time.
     */
    public static MethodHandle methodHandle(StandardErrorCode errorCode, Method method)
    {
        try {
            return MethodHandles.lookup().unreflect(method);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(errorCode, e);
        }
    }

    /**
     * Returns a MethodHandle corresponding to the specified method.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a method involves creating
     * JNI global weak references. G1 processes such references serially. As a result, calling this
     * method in a tight loop can create significant GC pressure and significantly increase
     * application pause time.
     */
    public static MethodHandle methodHandle(Method method)
    {
        return methodHandle(GENERIC_INTERNAL_ERROR, method);
    }

    /**
     * Returns a MethodHandle corresponding to the specified constructor.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a constructor involves
     * creating JNI global weak references. G1 processes such references serially. As a result,
     * calling this method in a tight loop can create significant GC pressure and significantly
     * increase application pause time.
     */
    public static MethodHandle constructorMethodHandle(Class<?> clazz, Class<?>... parameterTypes)
    {
        return constructorMethodHandle(GENERIC_INTERNAL_ERROR, clazz, parameterTypes);
    }

    /**
     * Returns a MethodHandle corresponding to the specified constructor.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a constructor involves
     * creating JNI global weak references. G1 processes such references serially. As a result,
     * calling this method in a tight loop can create significant GC pressure and significantly
     * increase application pause time.
     */
    public static MethodHandle constructorMethodHandle(StandardErrorCode errorCode, Class<?> clazz, Class<?>... parameterTypes)
    {
        try {
            return MethodHandles.lookup().unreflectConstructor(clazz.getConstructor(parameterTypes));
        }
        catch (IllegalAccessException | NoSuchMethodException e) {
            throw new PrestoException(errorCode, e);
        }
    }

    /**
     * Returns a MethodHandle corresponding to the specified constructor.
     * <p>
     * Warning: The way Oracle JVM implements producing MethodHandle for a constructor involves
     * creating JNI global weak references. G1 processes such references serially. As a result,
     * calling this method in a tight loop can create significant GC pressure and significantly
     * increase application pause time.
     */
    public static MethodHandle constructorMethodHandle(StandardErrorCode errorCode, Constructor constructor)
    {
        try {
            return MethodHandles.lookup().unreflectConstructor(constructor);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(errorCode, e);
        }
    }
}
