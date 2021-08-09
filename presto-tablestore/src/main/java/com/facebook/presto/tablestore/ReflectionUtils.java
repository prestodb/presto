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
package com.facebook.presto.tablestore;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class ReflectionUtils
{
    public static Optional<Field> findField(Class<?> clazz, String name)
    {
        return findField(clazz, name, Optional.empty());
    }

    public static Optional<Field> findField(Class<?> clazz, String name, Optional<Class<?>> type)
    {
        requireNonNull(clazz != null, "Class must not be null");
        requireNonNull(name != null, "Name of the field must not be null");

        Class<?> searchType = clazz;
        while (!Object.class.equals(searchType) && searchType != null) {
            Field[] fields = searchType.getDeclaredFields();
            for (Field field : fields) {
                if ((name == null || name.equals(field.getName())) && (!type.isPresent() || type.get().equals(field.getType()))) {
                    return Optional.of(field);
                }
            }
            searchType = searchType.getSuperclass();
        }
        return Optional.empty();
    }

    public static Object getField(Field field, Object target)
    {
        try {
            return field.get(target);
        }
        catch (IllegalAccessException exception) {
            throw new IllegalStateException("Unexpected reflection exception - " + exception.getClass().getName() + ": " + exception.getMessage());
        }
    }

    public static void makeAccessible(Field field)
    {
        if ((!Modifier.isPublic(field.getModifiers()) || !Modifier.isPublic(field.getDeclaringClass().getModifiers()) || Modifier.isFinal(field.getModifiers())) && !field.isAccessible()) {
            field.setAccessible(true);
        }
    }
}
