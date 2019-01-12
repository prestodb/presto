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

public final class Numbers
{
    private Numbers() {}

    public static boolean isFloatingNumber(Object number)
    {
        return number instanceof Double || number instanceof Float;
    }

    public static boolean isNan(Object number)
    {
        return number.equals(Double.NaN) || number.equals(Float.NaN);
    }
}
