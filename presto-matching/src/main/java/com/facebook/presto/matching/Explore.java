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
package com.facebook.presto.matching;

import com.facebook.presto.matching.pattern.EqualsPattern;
import com.facebook.presto.matching.pattern.FilterPattern;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class Explore<F, C, T>
{
    private final String name;
    private final BiFunction<F, C, Stream<T>> function;

    public static <F, C, T> Explore<F, C, T> explore(String name, Function<F, Stream<T>> function)
    {
        return new Explore<F, C, T>(name, (t, context) -> function.apply(t));
    }

    public static <F, C, T> Explore<F, C, T> explore(String name, BiFunction<F, C, Stream<T>> function)
    {
        return new Explore<F, C, T>(name, function);
    }

    public Explore(String name, BiFunction<F, C, Stream<T>> function)
    {
        this.name = requireNonNull(name, "name is null");
        this.function = requireNonNull(function, "function is null");
    }

    public String getName()
    {
        return name;
    }

    public BiFunction<F, C, Stream<?>> getFunction()
    {
        //without the ::apply below, the type system is unable to drop the R type from Optional
        return function::apply;
    }

    public <R> ExplorePattern<F, C, R> matching(Pattern<R> pattern)
    {
        return ExplorePattern.of(this, pattern);
    }

    public ExplorePattern<F, C, T> capturedAs(Capture<T> capture)
    {
        Pattern<T> matchAll = (Pattern<T>) Pattern.any();
        return matching(matchAll.capturedAs(capture));
    }

    public ExplorePattern<F, C, T> equalTo(T expectedValue)
    {
        return matching(new EqualsPattern<>(expectedValue, null));
    }

    public ExplorePattern<F, C, T> matching(Predicate<? super T> predicate)
    {
        return matching((t, context) -> predicate.test(t));
    }

    public ExplorePattern<F, C, T> matching(BiPredicate<? super T, ?> predicate)
    {
        return matching(new FilterPattern<>(predicate, null));
    }
}
