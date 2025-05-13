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
package org.apache.hadoop.fs;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class HadoopExtendedFileSystemCache
{
    private static PrestoExtendedFileSystemCache cache;

    private HadoopExtendedFileSystemCache() {}

    public static synchronized void initialize()
    {
        if (cache == null) {
            cache = setFinalStatic(FileSystem.class, "CACHE", new PrestoExtendedFileSystemCache());
        }
    }

    private static <T> T setFinalStatic(Class<?> clazz, String name, T value)
    {
        try {
            Field field = clazz.getDeclaredField(name);
            field.setAccessible(true);

            Field modifiersField = getModifiersField();
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, value);

            return value;
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Field getModifiersField() throws NoSuchFieldException
    {
        try {
            return Field.class.getDeclaredField("modifiers");
        }
        catch (NoSuchFieldException e) {
            try {
                Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class);
                getDeclaredFields0.setAccessible(true);
                Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false);
                for (Field field : fields) {
                    if ("modifiers".equals(field.getName())) {
                        return field;
                    }
                }
            }
            catch (ReflectiveOperationException ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }
}
