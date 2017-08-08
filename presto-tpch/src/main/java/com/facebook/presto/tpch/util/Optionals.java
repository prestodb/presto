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

package com.facebook.presto.tpch.util;

import java.util.Optional;
import java.util.function.BiFunction;

public class Optionals
{
    private Optionals() {}

    public static <S, T, R> Optional<R> withBoth(Optional<? extends S> left, Optional<? extends T> right, BiFunction<S, T, R> binaryFunction)
    {
        return left.flatMap(l -> right.map(r -> binaryFunction.apply(l, r)));
    }
}
