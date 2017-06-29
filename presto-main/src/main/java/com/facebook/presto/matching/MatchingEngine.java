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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;
import com.google.common.collect.TreeTraverser;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.util.MoreLists.asList;

public class MatchingEngine<T extends Matchable>
{
    private final ListMultimap<Class, T> matchablesByClass;

    private MatchingEngine(ListMultimap<Class, T> matchablesByClass)
    {
        this.matchablesByClass = ImmutableListMultimap.copyOf(matchablesByClass);
    }

    public Stream<T> getCandidates(Object object)
    {
        return Streams.stream(ancestors(object.getClass()))
                .flatMap(clazz -> matchablesByClass.get(clazz).stream());
    }

    private static Iterator<Class> ancestors(Class clazz)
    {
        return TreeTraverser.using((Class n) -> asList(Optional.ofNullable(n.getSuperclass())))
                .preOrderTraversal(clazz)
                .iterator();
    }

    public static <T extends Matchable> Builder<T> builder()
    {
        return new Builder<T>();
    }

    public static class Builder<T extends Matchable>
    {
        private final ImmutableListMultimap.Builder<Class, T> matchablesByClass = ImmutableListMultimap.builder();

        public Builder<T> register(Set<T> matchables)
        {
            matchables.forEach(this::register);
            return this;
        }

        public Builder<T> register(T matchable)
        {
            Pattern pattern = matchable.getPattern();
            if (pattern instanceof Pattern.MatchByClass) {
                matchablesByClass.put(((Pattern.MatchByClass) pattern).getObjectClass(), matchable);
            }
            else {
                throw new IllegalArgumentException("Unexpected Pattern: " + pattern);
            }
            return this;
        }

        public MatchingEngine<T> build()
        {
            return new MatchingEngine(matchablesByClass.build());
        }
    }
}
