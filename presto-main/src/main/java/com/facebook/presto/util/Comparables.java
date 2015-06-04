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

import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public final class Comparables
{
    private Comparables() {}

    public static <T extends Comparable<T>> T min(T... elements)
    {
        checkArgument(elements.length > 0, "requires one or more elements");
        return Stream.of(elements)
                .min(T::compareTo)
                .get();
    }

    public static <T extends Comparable<T>> T max(T... elements)
    {
        checkArgument(elements.length > 0, "requires one or more elements");
        return Stream.of(elements)
                .max(T::compareTo)
                .get();
    }

    public static <T extends Comparable<T>> Stream<T> sorted(T... elements)
    {
        return Stream.of(elements)
                .sorted(T::compareTo);
    }
}
