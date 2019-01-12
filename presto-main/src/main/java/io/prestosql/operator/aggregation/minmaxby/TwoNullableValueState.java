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
package io.prestosql.operator.aggregation.minmaxby;

import io.prestosql.operator.aggregation.state.InitialBooleanValue;
import io.prestosql.spi.function.AccumulatorState;

// TODO: Add multiple aggregation states support to aggregation framework to avoid
// cartesian product of types for aggregation functions takes multiple parameters.
// Deprecate this class once the support is added.
public interface TwoNullableValueState
        extends AccumulatorState
{
    @InitialBooleanValue(true)
    boolean isFirstNull();

    void setFirstNull(boolean firstNull);

    @InitialBooleanValue(true)
    boolean isSecondNull();

    void setSecondNull(boolean secondNull);
}
