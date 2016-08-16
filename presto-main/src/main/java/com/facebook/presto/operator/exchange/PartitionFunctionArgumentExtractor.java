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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PartitionFunctionArgumentExtractor
        implements Function<Page, Page>
{
    private final List<Integer> partitionChannels;
    private final List<Optional<Block>> partitionConstants;

    public PartitionFunctionArgumentExtractor(PartitioningScheme partitioningScheme, List<Symbol> layout)
    {
        requireNonNull(partitioningScheme, "partitioningScheme is null");
        requireNonNull(layout, "layout is null");

        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(layout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
        }
        else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(ArgumentBinding::getColumn)
                    .map(layout::indexOf)
                    .collect(toImmutableList());

            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return Optional.of(argument.getConstant().asBlock());
                        }
                        return Optional.<Block>empty();
                    })
                    .collect(toImmutableList());
        }
    }

    @Override
    public Page apply(Page page)
    {
        Block[] blocks = new Block[partitionChannels.size()];
        for (int i = 0; i < blocks.length; i++) {
            Optional<Block> partitionConstant = partitionConstants.get(i);
            if (partitionConstant.isPresent()) {
                blocks[i] = new RunLengthEncodedBlock(partitionConstant.get(), page.getPositionCount());
            }
            else {
                blocks[i] = page.getBlock(partitionChannels.get(i));
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
