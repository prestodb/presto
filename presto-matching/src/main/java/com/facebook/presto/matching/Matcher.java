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
import com.facebook.presto.matching.pattern.EqualsPattern;
import com.facebook.presto.matching.pattern.FilterPattern;
import com.facebook.presto.matching.pattern.TypeOfPattern;
import com.facebook.presto.matching.pattern.WithPattern;

public interface Matcher
{
    default <T> Match<T> match(Pattern<T> pattern, Object object)
    {
        return match(pattern, object, Captures.empty());
    }

    <T> Match<T> match(Pattern<T> pattern, Object object, Captures captures);

    <T> Match<T> matchTypeOf(TypeOfPattern<T> typeOfPattern, Object object, Captures captures);

    <T> Match<T> matchWith(WithPattern<T> withPattern, Object object, Captures captures);

    <T> Match<T> matchCapture(CapturePattern<T> capturePattern, Object object, Captures captures);

    <T> Match<T> matchEquals(EqualsPattern<T> equalsPattern, Object object, Captures captures);

    <T> Match<T> matchFilter(FilterPattern<T> filterPattern, Object object, Captures captures);
}
