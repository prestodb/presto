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

package io.prestosql.util;

import java.util.Optional;
import java.util.function.BinaryOperator;

public class Optionals
{
    private Optionals() {}

    public static <T> Optional<T> combine(Optional<T> left, Optional<T> right, BinaryOperator<T> combiner)
    {
        if (left.isPresent() && right.isPresent()) {
            return Optional.of(combiner.apply(left.get(), right.get()));
        }
        else if (left.isPresent()) {
            return left;
        }
        else {
            return right;
        }
    }
}
