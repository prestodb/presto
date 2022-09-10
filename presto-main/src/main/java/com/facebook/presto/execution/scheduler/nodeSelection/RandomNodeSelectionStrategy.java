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

import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.metadata.InternalNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.List;

/**
 * This NodeSelector makes a selection from the candidates randomly. The selection
 * is strictly random and based on ResettableRandomizedIterator
 */
public class RandomNodeSelectionStrategy
        implements NodeSelectionStrategy
{
    @Override
    public List<InternalNode> select(List<InternalNode> candidates, NodeSelectionHint hint)
    {
        return Streams.stream(new ResettableRandomizedIterator<>(candidates))
                .filter(node -> hint.canIncludeCoordinator() || !node.isCoordinator())
                .filter(node -> !hint.hasExclusionSet() || !hint.getExclusionSet().contains(node))
                .limit(hint.getLimit().orElse(Long.MAX_VALUE))
                .collect(ImmutableList.toImmutableList());
    }
}
