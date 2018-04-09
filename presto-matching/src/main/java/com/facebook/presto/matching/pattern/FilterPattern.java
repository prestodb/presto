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
import com.facebook.presto.matching.Matcher;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.PatternVisitor;

import java.util.function.Predicate;

public class FilterPattern<T>
        extends Pattern<T>
{
    private final Predicate<? super T> predicate;

    public FilterPattern(Predicate<? super T> predicate, Pattern<T> previous)
    {
        super(previous);
        this.predicate = predicate;
    }

    public Predicate<? super T> predicate()
    {
        return predicate;
    }

    @Override
    public Match<T> accept(Matcher matcher, Object object, Captures captures)
    {
        return matcher.matchFilter(this, object, captures);
    }

    @Override
    public void accept(PatternVisitor patternVisitor)
    {
        patternVisitor.visitFilter(this);
    }
}
