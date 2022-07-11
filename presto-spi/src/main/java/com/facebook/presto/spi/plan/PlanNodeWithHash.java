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
package com.facebook.presto.spi.plan;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PlanNodeWithHash
{
    private final PlanNode planNode;
    // An optional canonical hash of the corresponding plan node
    private final Optional<String> hash;

    public PlanNodeWithHash(PlanNode planNode, Optional<String> hash)
    {
        this.planNode = requireNonNull(planNode, "planNode is null");
        this.hash = requireNonNull(hash, "hash is null");
    }

    public PlanNode getPlanNode()
    {
        return planNode;
    }

    public Optional<String> getHash()
    {
        return hash;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeWithHash that = (PlanNodeWithHash) o;
        return Objects.equals(planNode, that.planNode) && Objects.equals(hash, that.hash);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(planNode, hash);
    }

    @Override
    public String toString()
    {
        return format("plan: %s, hash: %s", planNode, hash);
    }
}
