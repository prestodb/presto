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
package io.prestosql.plugin.tpch.util;

import static com.google.common.base.Preconditions.checkArgument;

public class Types
{
    private Types() {}

    public static <T> T checkType(Object object, Class<T> expectedClass)
    {
        return checkType(object, expectedClass, "Expected an object of type [%s]", expectedClass.getCanonicalName());
    }

    public static <T> T checkType(Object object, Class<T> expectedClass, String messageTemplate, Object... arguments)
    {
        checkArgument(expectedClass.isInstance(object), messageTemplate, arguments);
        return expectedClass.cast(object);
    }

    public static void checkSameType(Object left, Object right)
    {
        Class<?> leftClass = left.getClass();
        Class<?> rightClass = right.getClass();
        checkArgument(leftClass.equals(rightClass), "Values must be of same type, got [%s : %s] and [%s : %s]", left, leftClass, right, rightClass);
    }
}
