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

import static java.util.Objects.requireNonNull;

public class ExplorePattern<F, R>
{
    private final Explore<F, ?> explore;
    private final Pattern<R> pattern;

    public static <F, T, R> ExplorePattern<F, R> of(Explore<F, T> explore, Pattern<R> pattern)
    {
        return new ExplorePattern<>(explore, pattern);
    }

    private ExplorePattern(Explore<F, ?> explore, Pattern<R> pattern)
    {
        this.explore = requireNonNull(explore, "explore is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
    }

    public Explore<F, ?> getExplore()
    {
        return explore;
    }

    public Pattern<R> getPattern()
    {
        return pattern;
    }
}
