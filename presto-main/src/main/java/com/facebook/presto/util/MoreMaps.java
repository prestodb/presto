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

import java.util.Collection;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public final class MoreMaps
{
    private MoreMaps() {}

    public static <K, V> Map<K, V> mergeMaps(Map<K, V> map1, Map<K, V> map2, BinaryOperator<V> merger)
    {
        return Stream.of(map1, map2)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, merger));
    }
}
