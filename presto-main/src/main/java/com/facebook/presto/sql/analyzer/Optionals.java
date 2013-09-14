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
package com.facebook.presto.sql.analyzer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

public class Optionals
{
    public static Predicate<Optional<?>> isPresentPredicate()
    {
        return new Predicate<Optional<?>>()
        {
            @Override
            public boolean apply(Optional<?> input)
            {
                return input.isPresent();
            }
        };
    }

    public static <T> Function<Optional<T>, T> optionalGetter()
    {
        return new Function<Optional<T>, T>()
        {
            @Override
            public T apply(Optional<T> input)
            {
                return input.get();
            }
        };
    }
}
