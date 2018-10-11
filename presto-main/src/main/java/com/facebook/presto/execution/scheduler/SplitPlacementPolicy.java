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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;

import java.util.List;
import java.util.Set;

public interface SplitPlacementPolicy
{
    SplitPlacementResult computeAssignments(SplitPlacementSet splits);

    void lockDownNodes();

    List<Node> allNodes();

    default Node getNodeForLifespan(Lifespan lifespan)
    {
        throw new UnsupportedOperationException();
    }

    class SplitPlacementSet
    {
        private final Set<Split> splits;
        private final Lifespan lifespan;

        public SplitPlacementSet(Set<Split> splits, Lifespan lifespan)
        {
            this.splits = splits;
            this.lifespan = lifespan;
        }

        public Set<Split> getSplits()
        {
            return splits;
        }

        public Lifespan getLifespan()
        {
            return lifespan;
        }
    }
}
