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

import com.facebook.presto.matching.pattern.FilterPattern;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Property<F, T>
{
    static <F, T> Property<F, T> property(Function<F, T> property)
    {
        return optionalProperty(source -> Optional.of(property.apply(source)));
    }

    static <F, T> Property<F, T> optionalProperty(Function<F, Optional<T>> property)
    {
        return new Property<F, T>()
        {
            @Override
            public <R> PropertyPattern<F, R> matching(Pattern<R> pattern)
            {
                return PropertyPattern.of(property, pattern);
            }
        };
    }

    <R> PropertyPattern<F, R> matching(Pattern<R> pattern);

    default PropertyPattern<F, T> capturedAs(Capture<T> capture)
    {
        Pattern<T> matchAll = (Pattern<T>) Pattern.any();
        return matching(matchAll.capturedAs(capture));
    }

    default PropertyPattern<F, T> matching(Predicate<? super T> predicate)
    {
        return matching(new FilterPattern<>(predicate, null));
    }
}
