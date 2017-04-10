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
package com.facebook.presto.sql.planner.optimizations;

import java.util.function.Predicate;

public class Predicates
{
    private Predicates() {}

    public static <T> Predicate<T> isInstanceOfAny(Class... classes)
    {
        Predicate predicate = alwaysFalse();
        for (Class clazz : classes) {
            predicate = predicate.or(clazz::isInstance);
        }
        return predicate;
    }

    public static <T> Predicate<T> alwaysTrue()
    {
        return x -> true;
    }

    public static <T> Predicate<T> alwaysFalse()
    {
        return x -> false;
    }
}
