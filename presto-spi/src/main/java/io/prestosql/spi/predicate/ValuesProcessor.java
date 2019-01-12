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
package io.prestosql.spi.predicate;

import java.util.function.Consumer;
import java.util.function.Function;

public interface ValuesProcessor
{
    <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> discreteValuesFunction, Function<AllOrNone, T> allOrNoneFunction);

    void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> discreteValuesConsumer, Consumer<AllOrNone> allOrNoneConsumer);
}
