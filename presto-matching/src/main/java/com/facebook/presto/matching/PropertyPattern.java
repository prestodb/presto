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

public class PropertyPattern<F, R>
{
    private final Property<F, ?> property;
    private final Pattern<R> pattern;

    public static <F, T, R> PropertyPattern<F, R> of(Property<F, T> property, Pattern<R> pattern)
    {
        return new PropertyPattern<>(property, pattern);
    }

    private PropertyPattern(Property<F, ?> property, Pattern<R> pattern)
    {
        this.property = property;
        this.pattern = pattern;
    }

    public Property<F, ?> getProperty()
    {
        return property;
    }

    public Pattern<R> getPattern()
    {
        return pattern;
    }

    //This expresses the fact that PropertyPattern<F, T> is covariant on T.
    @SuppressWarnings("unchecked cast")
    public static <F, T> PropertyPattern<F, T> upcast(PropertyPattern<F, ? extends T> propertyPattern)
    {
        return (PropertyPattern<F, T>) propertyPattern;
    }
}
