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
package io.prestosql.orc;

import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static com.google.common.base.MoreObjects.toStringHelper;

public class OrcWriterFlushStats
{
    private final String name;
    private final DistributionStat stripeBytes = new DistributionStat();
    private final DistributionStat stripeRows = new DistributionStat();
    private final DistributionStat dictionaryBytes = new DistributionStat();

    public OrcWriterFlushStats(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    @Managed
    @Nested
    public DistributionStat getStripeBytes()
    {
        return stripeBytes;
    }

    @Managed
    @Nested
    public DistributionStat getStripeRows()
    {
        return stripeRows;
    }

    @Managed
    @Nested
    public DistributionStat getDictionaryBytes()
    {
        return dictionaryBytes;
    }

    public void recordStripeWritten(long stripeBytes, int stripeRows, int dictionaryBytes)
    {
        this.stripeBytes.add(stripeBytes);
        this.stripeRows.add(stripeRows);
        this.dictionaryBytes.add(dictionaryBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("stripeBytes", stripeBytes)
                .add("stripeRows", stripeRows)
                .add("dictionaryBytes", dictionaryBytes)
                .toString();
    }
}
