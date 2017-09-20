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
package com.facebook.presto.operator.project;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.DictionaryId;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DictionaryAwarePageProjection
        implements PageProjection
{
    private final PageProjection projection;
    private final Function<DictionaryBlock, DictionaryId> sourceIdFunction;

    private Block lastInputDictionary;
    private Optional<Block> lastOutputDictionary;
    private long lastDictionaryUsageCount;

    public DictionaryAwarePageProjection(PageProjection projection, Function<DictionaryBlock, DictionaryId> sourceIdFunction)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.sourceIdFunction = sourceIdFunction;
        verify(projection.isDeterministic(), "projection must be deterministic");
        verify(projection.getInputChannels().size() == 1, "projection must have only one input");
    }

    @Override
    public Type getType()
    {
        return projection.getType();
    }

    @Override
    public boolean isDeterministic()
    {
        return projection.isDeterministic();
    }

    @Override
    public InputChannels getInputChannels()
    {
        return projection.getInputChannels();
    }

    @Override
    public Block project(ConnectorSession session, Page page, SelectedPositions selectedPositions)
    {
        Block block = page.getBlock(0);
        if (block instanceof LazyBlock) {
            block = ((LazyBlock) block).getBlock();
        }

        if (block instanceof RunLengthEncodedBlock) {
            Block value = ((RunLengthEncodedBlock) block).getValue();
            Optional<Block> projectedValue = processDictionary(session, value);
            // single value block is always considered effective, but the processing could have thrown
            // in that case we fallback and process again so the correct error message sent
            if (projectedValue.isPresent()) {
                return new RunLengthEncodedBlock(projectedValue.get(), selectedPositions.size());
            }
        }

        if (block instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
            // Attempt to process the dictionary.  If dictionary is processing has not been considered effective, an empty response will be returned
            Optional<Block> projectedDictionary = processDictionary(session, dictionaryBlock.getDictionary());
            // record the usage count regardless of dictionary processing choice, so we have stats for next time
            lastDictionaryUsageCount += selectedPositions.size();
            // if dictionary was processed, produce a dictionary block; otherwise do normal processing
            if (projectedDictionary.isPresent()) {
                int[] outputIds = filterDictionaryIds(dictionaryBlock, selectedPositions);
                return new DictionaryBlock(selectedPositions.size(), projectedDictionary.get(), outputIds, false, sourceIdFunction.apply(dictionaryBlock));
            }
        }

        return projection.project(session, new Page(block), selectedPositions);
    }

    private Optional<Block> processDictionary(ConnectorSession session, Block dictionary)
    {
        if (lastInputDictionary == dictionary) {
            return lastOutputDictionary;
        }

        // Process dictionary if:
        //   this is the first block
        //   there is only entry in the dictionary
        //   the last dictionary was used for more positions than were in the dictionary
        boolean shouldProcessDictionary = lastInputDictionary == null || dictionary.getPositionCount() == 1 || lastDictionaryUsageCount >= lastInputDictionary.getPositionCount();

        lastDictionaryUsageCount = 0;
        lastInputDictionary = dictionary;

        if (shouldProcessDictionary) {
            try {
                lastOutputDictionary = Optional.of(projection.project(session, new Page(dictionary), SelectedPositions.positionsRange(0, dictionary.getPositionCount())));
            }
            catch (Exception ignored) {
                // Processing of dictionary failed, but we ignore the exception here
                // and force reprocessing of the whole block using the normal code.
                // The second pass may not fail due to filtering.
                // todo dictionary processing should be able to tolerate failures of unused elements
                lastOutputDictionary = Optional.empty();
            }
        }
        else {
            lastOutputDictionary = Optional.empty();
        }
        return lastOutputDictionary;
    }

    private static int[] filterDictionaryIds(DictionaryBlock dictionaryBlock, SelectedPositions selectedPositions)
    {
        int[] outputIds = new int[selectedPositions.size()];
        if (selectedPositions.isList()) {
            int[] positions = selectedPositions.getPositions();
            int endPosition = selectedPositions.getOffset() + selectedPositions.size();
            int outputIndex = 0;
            for (int position = selectedPositions.getOffset(); position < endPosition; position++) {
                outputIds[outputIndex++] = dictionaryBlock.getId(positions[position]);
            }
        }
        else {
            int endPosition = selectedPositions.getOffset() + selectedPositions.size();
            int outputIndex = 0;
            for (int position = selectedPositions.getOffset(); position < endPosition; position++) {
                outputIds[outputIndex++] = dictionaryBlock.getId(position);
            }
        }
        return outputIds;
    }
}
