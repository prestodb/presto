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

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

public class Property<F, C, T>
{
    private final String name;
    private final BiFunction<F, C, Optional<T>> function;

    public static <F, C, T> Property<F, C, T> property(String name, Function<F, T> function)
    {
        return property(name, (source, context) -> function.apply(source));
    }

    public static <F, C, T> Property<F, C, T> property(String name, BiFunction<F, C, T> function)
    {
        return optionalProperty(name, (source, context) -> Optional.of(function.apply(source, context)));
    }

    public static <F, C, T> Property<F, C, T> optionalProperty(String name, Function<F, Optional<T>> function)
    {
        return new Property<>(name, (source, context) -> function.apply(source));
    }

    public static <F, C, T> Property<F, C, T> optionalProperty(String name, BiFunction<F, C, Optional<T>> function)
    {
        return new Property<>(name, function);
    }

    public Property(String name, BiFunction<F, C, Optional<T>> function)
    {
        this.name = name;
        this.function = function;
    }

    public String getName()
    {
        return name;
    }

    public BiFunction<F, C, Optional<?>> getFunction()
    {
        //without the ::apply below, the type system is unable to drop the R type from Optional
        return function::apply;
    }

    public <R> PropertyPattern<F, C, R> matching(Pattern<R> pattern)
    {
        return PropertyPattern.of(this, pattern);
    }

    public PropertyPattern<F, C, T> capturedAs(Capture<T> capture)
    {
        Pattern<T> matchAll = (Pattern<T>) Pattern.any();
        return matching(matchAll.capturedAs(capture));
    }

    public PropertyPattern<F, C, T> equalTo(T expectedValue)
    {
        return matching(new EqualsPattern<>(expectedValue, null));
    }

    public PropertyPattern<F, C, T> matching(Predicate<? super T> predicate)
    {
        return matching((t, context) -> predicate.test(t));
    }

    public PropertyPattern<F, C, T> matching(BiPredicate<? super T, ?> predicate)
    {
        return matching(new FilterPattern<>(predicate, null));
    }
}
