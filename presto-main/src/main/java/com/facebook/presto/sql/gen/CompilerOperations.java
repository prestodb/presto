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
package com.facebook.presto.sql.gen;

// This methods are statically bound by the compiler
@SuppressWarnings("UnusedDeclaration")
public final class CompilerOperations
{
    private CompilerOperations()
    {
    }

    public static boolean and(boolean left, boolean right)
    {
        return left && right;
    }

    public static boolean or(boolean left, boolean right)
    {
        return left || right;
    }

    public static boolean not(boolean value)
    {
        return !value;
    }

    public static boolean lessThan(int left, int right)
    {
        return left < right;
    }
}
