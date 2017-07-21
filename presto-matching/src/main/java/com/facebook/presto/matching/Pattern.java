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
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.facebook.presto.matching.pattern.WithPattern;

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

    public Pattern<T> capturedAs(Capture<T> capture)
    {
        return new CapturePattern<>(capture, this);
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

    @Override
    public String toString()
    {
        DefaultPrinter printer = new DefaultPrinter();
        accept(printer);
        return printer.result();
    }
}
