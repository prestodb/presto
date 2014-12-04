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
package com.facebook.presto.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.stream.Collector;

public final class ImmutableCollectors
{
    private ImmutableCollectors() {}

    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> immutableListCollector()
    {
        return Collector.<T, ImmutableList.Builder<T>, ImmutableList<T>>of(
                ImmutableList.Builder::new,
                ImmutableList.Builder::add,
                (ImmutableList.Builder<T> left, ImmutableList.Builder<T> right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableList.Builder::build);
    }

    public static <T> Collector<T, ImmutableSet.Builder<T>, ImmutableSet<T>> immutableSetCollector()
    {
        return Collector.<T, ImmutableSet.Builder<T>, ImmutableSet<T>>of(
                ImmutableSet.Builder::new,
                ImmutableSet.Builder::add,
                (ImmutableSet.Builder<T> left, ImmutableSet.Builder<T> right) -> {
                    left.addAll(right.build());
                    return left;
                },
                ImmutableSet.Builder::build);
    }
}
