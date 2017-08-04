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

import java.util.Optional;
import java.util.function.Function;

public class PropertyPattern<F, R>
{
    private final Function<F, Optional<?>> property;
    private final Pattern<R> pattern;

    public static <F, T, R> PropertyPattern<F, R> of(Function<F, Optional<T>> property, Pattern<R> pattern)
    {
        //without the ::apply below, the type system is unable to drop the R type from Optional
        return new PropertyPattern<>(property::apply, pattern);
    }

    private PropertyPattern(Function<F, Optional<?>> property, Pattern<R> pattern)
    {
        this.property = property;
        this.pattern = pattern;
    }

    public Function<F, Optional<?>> getProperty()
    {
        return property;
    }

    public Pattern<R> getPattern()
    {
        return pattern;
    }
}
