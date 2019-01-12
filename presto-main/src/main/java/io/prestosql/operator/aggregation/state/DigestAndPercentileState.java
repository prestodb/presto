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

import io.airlift.stats.QuantileDigest;
import io.prestosql.spi.function.AccumulatorState;
import io.prestosql.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(stateSerializerClass = DigestAndPercentileStateSerializer.class, stateFactoryClass = DigestAndPercentileStateFactory.class)
public interface DigestAndPercentileState
        extends AccumulatorState
{
    QuantileDigest getDigest();

    void setDigest(QuantileDigest digest);

    double getPercentile();

    void setPercentile(double percentile);

    void addMemoryUsage(int value);
}
