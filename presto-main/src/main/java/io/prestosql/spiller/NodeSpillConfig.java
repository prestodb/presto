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
package io.prestosql.spiller;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

public class NodeSpillConfig
{
    private DataSize maxSpillPerNode = new DataSize(100, DataSize.Unit.GIGABYTE);
    private DataSize queryMaxSpillPerNode = new DataSize(100, DataSize.Unit.GIGABYTE);

    @NotNull
    public DataSize getMaxSpillPerNode()
    {
        return maxSpillPerNode;
    }

    @Config("experimental.max-spill-per-node")
    public NodeSpillConfig setMaxSpillPerNode(DataSize maxSpillPerNode)
    {
        this.maxSpillPerNode = maxSpillPerNode;
        return this;
    }

    @NotNull
    public DataSize getQueryMaxSpillPerNode()
    {
        return queryMaxSpillPerNode;
    }

    @Config("experimental.query-max-spill-per-node")
    public NodeSpillConfig setQueryMaxSpillPerNode(DataSize queryMaxSpillPerNode)
    {
        this.queryMaxSpillPerNode = queryMaxSpillPerNode;
        return this;
    }
}
