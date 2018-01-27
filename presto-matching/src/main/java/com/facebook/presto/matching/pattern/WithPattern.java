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
package com.facebook.presto.matching.pattern;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.PatternVisitor;
import com.facebook.presto.matching.Property;
import com.facebook.presto.matching.PropertyPattern;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class WithPattern<T>
        extends Pattern<T>
{
    private final PropertyPattern<? super T, ?, ?> propertyPattern;

    public WithPattern(PropertyPattern<? super T, ?, ?> propertyPattern, Pattern<T> previous)
    {
        super(previous);
        this.propertyPattern = requireNonNull(propertyPattern, "propertyPattern is null");
    }

    public Pattern<?> getPattern()
    {
        return propertyPattern.getPattern();
    }

    public Property<? super T, ?, ?> getProperty()
    {
        return propertyPattern.getProperty();
    }

    @Override
    public <C> Stream<Match> accept(Object object, Captures captures, C context)
    {
        //TODO remove cast
        BiFunction<? super T, C, Optional<?>> property = (BiFunction<? super T, C, Optional<?>>) propertyPattern.getProperty().getFunction();
        Optional<?> propertyValue = property.apply((T) object, context);
        return propertyValue.map(value -> propertyPattern.getPattern().match(value, captures, context))
                .orElse(Stream.of());
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitWith(this);
    }
}
