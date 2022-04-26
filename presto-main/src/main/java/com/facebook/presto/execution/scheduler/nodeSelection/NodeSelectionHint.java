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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.google.common.base.Preconditions;

import java.util.OptionalLong;

/**
 * Class helps to provide suitable hint to the selection algorithm
 * in the NodeSelector.
 */
public class NodeSelectionHint
{
    private final OptionalLong limit;
    private final boolean includeCoordinator;

    public NodeSelectionHint(OptionalLong limit, boolean includeCoordinator)
    {
        this.limit = limit;
        this.includeCoordinator = includeCoordinator;
    }

    public OptionalLong getLimit()
    {
        return limit;
    }

    public boolean canIncludeCoordinator()
    {
        return includeCoordinator;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private OptionalLong limit = OptionalLong.empty();
        private boolean includeCoordinator = true;

        public Builder limit(long limit)
        {
            Preconditions.checkArgument(limit > 0, "Limit must be positive");
            this.limit = OptionalLong.of(limit);
            return this;
        }

        public Builder includeCoordinator(boolean flag)
        {
            this.includeCoordinator = flag;
            return this;
        }

        public NodeSelectionHint build()
        {
            return new NodeSelectionHint(limit, includeCoordinator);
        }
    }
}
