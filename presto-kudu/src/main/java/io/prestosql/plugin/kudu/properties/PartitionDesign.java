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
package io.prestosql.plugin.kudu.properties;

import java.util.List;

public class PartitionDesign
{
    private List<HashPartitionDefinition> hash;
    private RangePartitionDefinition range;

    public List<HashPartitionDefinition> getHash()
    {
        return hash;
    }

    public void setHash(List<HashPartitionDefinition> hash)
    {
        this.hash = hash;
    }

    public RangePartitionDefinition getRange()
    {
        return range;
    }

    public void setRange(RangePartitionDefinition range)
    {
        this.range = range;
    }

    public boolean hasPartitions()
    {
        return hash != null && !hash.isEmpty() && !hash.get(0).getColumns().isEmpty()
                || range != null && !range.getColumns().isEmpty();
    }
}
