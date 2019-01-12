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
package io.prestosql.plugin.hive;

import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class HiveWriterStats
{
    private final DistributionStat inputPageSizeInBytes = new DistributionStat();

    @Managed
    @Nested
    public DistributionStat getInputPageSizeInBytes()
    {
        return inputPageSizeInBytes;
    }

    public void addInputPageSizesInBytes(long bytes)
    {
        inputPageSizeInBytes.add(bytes);
    }
}
