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

import java.util.stream.Stream;

public interface Matcher
{
    default <T> Stream<Match> match(Pattern<T> pattern, Object object)
    {
        return match(pattern, object, Captures.empty(), noContext());
    }

    static Void noContext()
    {
        return null;
    }

    default <T, C> Stream<Match> match(Pattern<T> pattern, Object object, C context)
    {
        return match(pattern, object, Captures.empty(), context);
    }

    <T, C> Stream<Match> match(Pattern<T> pattern, Object object, Captures captures, C context);
}
