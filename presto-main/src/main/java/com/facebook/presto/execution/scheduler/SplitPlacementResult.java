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

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;
import com.google.common.collect.Multimap;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public final class SplitPlacementResult
{
    private final CompletableFuture<?> blocked;
    private final Multimap<Node, Split> assignments;

    public SplitPlacementResult(CompletableFuture<?> blocked, Multimap<Node, Split> assignments)
    {
        this.blocked = requireNonNull(blocked, "blocked is null");
        this.assignments = requireNonNull(assignments, "assignments is null");
    }

    public CompletableFuture<?> getBlocked()
    {
        return blocked;
    }

    public Multimap<Node, Split> getAssignments()
    {
        return assignments;
    }
}
