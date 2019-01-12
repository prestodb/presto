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
package io.prestosql.operator.aggregation.state;

import io.prestosql.spi.function.AccumulatorState;

public interface CentralMomentsState
        extends AccumulatorState
{
    long getCount();

    void setCount(long value);

    double getM1();

    void setM1(double value);

    double getM2();

    void setM2(double value);

    double getM3();

    void setM3(double value);

    double getM4();

    void setM4(double value);
}
