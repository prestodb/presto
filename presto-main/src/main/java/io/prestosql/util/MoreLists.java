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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MoreLists
{
    public static <T> List<List<T>> listOfListsCopy(List<List<T>> lists)
    {
        return requireNonNull(lists, "lists is null").stream()
                .map(ImmutableList::copyOf)
                .collect(toImmutableList());
    }

    public static <T> List<T> filteredCopy(List<T> elements, Predicate<T> predicate)
    {
        requireNonNull(elements, "elements is null");
        requireNonNull(predicate, "predicate is null");
        return elements.stream()
                .filter(predicate)
                .collect(toImmutableList());
    }

    public static <T, R> List<R> mappedCopy(List<T> elements, Function<T, R> mapper)
    {
        requireNonNull(elements, "elements is null");
        requireNonNull(mapper, "mapper is null");
        return elements.stream()
                .map(mapper)
                .collect(toImmutableList());
    }

    public static <T> List<T> nElements(int n, IntFunction<T> function)
    {
        checkArgument(n >= 0, "n must be greater than or equal to zero");
        requireNonNull(function, "function is null");
        return IntStream.range(0, n)
                .mapToObj(function)
                .collect(toImmutableList());
    }

    private MoreLists() {}
}
