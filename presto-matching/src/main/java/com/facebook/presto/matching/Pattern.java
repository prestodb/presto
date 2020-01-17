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

import com.facebook.presto.matching.pattern.CapturePattern;
import com.facebook.presto.matching.pattern.FilterPattern;
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.facebook.presto.matching.pattern.WithPattern;
import com.google.common.collect.Iterables;

import java.util.function.Predicate;

import static com.facebook.presto.matching.DefaultMatcher.DEFAULT_MATCHER;
import static com.google.common.base.Predicates.not;

public abstract class Pattern<T>
{
    private final Pattern<?> previous;

    public static Pattern<Object> any()
    {
        return typeOf(Object.class);
    }

    public static <T> Pattern<T> typeOf(Class<T> expectedClass)
    {
        return new TypeOfPattern<>(expectedClass);
    }

    protected Pattern()
    {
        this(null);
    }

    protected Pattern(Pattern<?> previous)
    {
        this.previous = previous;
    }

    //FIXME make sure there's a proper toString,
    // like with(propName)\n\tfilter(isEmpty)
    // or with(propName) map(isEmpty) equalTo(true)
    public static <F, T extends Iterable<S>, S> PropertyPattern<F, T> empty(Property<F, T> property)
    {
        return PropertyPattern.upcast(property.matching(Iterables::isEmpty));
    }

    public static <F, T extends Iterable<S>, S> PropertyPattern<F, T> nonEmpty(Property<F, T> property)
    {
        return PropertyPattern.upcast(property.matching(not(Iterables::isEmpty)));
    }

    public Pattern<T> capturedAs(Capture<T> capture)
    {
        return new CapturePattern<>(capture, this);
    }

    public Pattern<T> matching(Predicate<? super T> predicate)
    {
        return new FilterPattern<>(predicate, this);
    }

    public Pattern<T> with(PropertyPattern<? super T, ?> pattern)
    {
        return new WithPattern<>(pattern, this);
    }

    public Pattern<?> previous()
    {
        return previous;
    }

    public abstract Match<T> accept(Matcher matcher, Object object, Captures captures);

    public abstract void accept(PatternVisitor patternVisitor);

    public boolean matches(Object object)
    {
        return DEFAULT_MATCHER.match(this, object).isPresent();
    }

    @Override
    public String toString()
    {
        DefaultPrinter printer = new DefaultPrinter();
        accept(printer);
        return printer.result();
    }
}
