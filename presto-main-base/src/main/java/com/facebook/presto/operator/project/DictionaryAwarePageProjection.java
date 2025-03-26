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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.DictionaryId;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.CompletedWork;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DictionaryAwarePageProjection
        implements PageProjection
{
    private final PageProjection projection;
    private final Function<DictionaryBlock, DictionaryId> sourceIdFunction;

    private Block lastInputDictionary;
    private Optional<List<Block>> lastOutputDictionary;
    private long lastDictionaryUsageCount;

    public DictionaryAwarePageProjection(PageProjection projection, Function<DictionaryBlock, DictionaryId> sourceIdFunction)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.sourceIdFunction = sourceIdFunction;
        verify(projection.isDeterministic(), "projection must be deterministic");
        verify(projection.getInputChannels().size() == 1, "projection must have only one input");
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
    public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return new DictionaryAwarePageProjectionWork(properties, yieldSignal, page, selectedPositions);
    }

    private class DictionaryAwarePageProjectionWork
            implements Work<List<Block>>
    {
        private final SqlFunctionProperties properties;
        private final DriverYieldSignal yieldSignal;
        private final Block block;
        private final SelectedPositions selectedPositions;

        private List<Block> results;
        // if the block is RLE or dictionary block, we may use dictionary processing
        private Work<List<Block>> dictionaryProcessingProjectionWork;
        // always prepare to fall back to a general block in case the dictionary does not apply or fails
        private Work<List<Block>> fallbackProcessingProjectionWork;

        public DictionaryAwarePageProjectionWork(@Nullable SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            this.properties = properties;
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");

            Block block = requireNonNull(page, "page is null").getBlock(0).getLoadedBlock();
            this.block = block;
            this.selectedPositions = requireNonNull(selectedPositions, "selectedPositions is null");

            Optional<Block> dictionary = Optional.empty();
            if (block instanceof RunLengthEncodedBlock) {
                dictionary = Optional.of(((RunLengthEncodedBlock) block).getValue());
            }
            else if (block instanceof DictionaryBlock) {
                dictionary = Optional.of(((DictionaryBlock) block).getDictionary());
            }

            // Try use dictionary processing first; if it fails, fall back to the generic case
            dictionaryProcessingProjectionWork = createDictionaryBlockProjection(dictionary);
            fallbackProcessingProjectionWork = null;
        }

        @Override
        public boolean process()
        {
            checkState(results == null, "result has been generated");
            if (fallbackProcessingProjectionWork != null) {
                if (fallbackProcessingProjectionWork.process()) {
                    results = fallbackProcessingProjectionWork.getResult();
                    return true;
                }
                return false;
            }

            Optional<List<Block>> dictionaryOutput = Optional.empty();
            if (dictionaryProcessingProjectionWork != null) {
                try {
                    if (!dictionaryProcessingProjectionWork.process()) {
                        // dictionary processing yielded.
                        return false;
                    }
                    dictionaryOutput = Optional.of(dictionaryProcessingProjectionWork.getResult());
                    lastOutputDictionary = dictionaryOutput;
                }
                catch (Exception ignored) {
                    // Processing of dictionary failed, but we ignore the exception here
                    // and force reprocessing of the whole block using the normal code.
                    // The second pass may not fail due to filtering.
                    // todo dictionary processing should be able to tolerate failures of unused elements
                    lastOutputDictionary = Optional.empty();
                    dictionaryProcessingProjectionWork = null;
                }
            }

            if (block instanceof DictionaryBlock) {
                // Record the usage count regardless of dictionary processing choice, so we have stats for next time.
                // This guarantees recording will happen once and only once regardless of whether dictionary processing was attempted and whether it succeeded.
                lastDictionaryUsageCount += selectedPositions.size();
            }

            if (dictionaryOutput.isPresent()) {
                if (block instanceof RunLengthEncodedBlock) {
                    // single value block is always considered effective, but the processing could have thrown
                    // in that case we fallback and process again so the correct error message sent
                    results = dictionaryOutput.get().stream()
                            .map(block -> new RunLengthEncodedBlock(block, selectedPositions.size()))
                            .collect(toImmutableList());
                    return true;
                }

                if (block instanceof DictionaryBlock) {
                    DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                    // if dictionary was processed, produce a dictionary block; otherwise do normal processing
                    int[] outputIds = filterDictionaryIds(dictionaryBlock, selectedPositions);
                    results = dictionaryOutput.get().stream()
                            .map(block -> new DictionaryBlock(selectedPositions.size(), block, outputIds, false, sourceIdFunction.apply(dictionaryBlock)))
                            .collect(toImmutableList());
                    return true;
                }

                throw new UnsupportedOperationException("unexpected block type " + block.getClass());
            }

            // there is no dictionary handling or dictionary handling failed; fall back to general projection
            verify(dictionaryProcessingProjectionWork == null);
            verify(fallbackProcessingProjectionWork == null);
            fallbackProcessingProjectionWork = projection.project(properties, yieldSignal, new Page(block), selectedPositions);
            if (fallbackProcessingProjectionWork.process()) {
                results = fallbackProcessingProjectionWork.getResult();
                return true;
            }
            return false;
        }

        @Override
        public List<Block> getResult()
        {
            checkState(results != null, "result has not been generated");
            return results;
        }

        private Work<List<Block>> createDictionaryBlockProjection(Optional<Block> dictionary)
        {
            if (!dictionary.isPresent()) {
                lastOutputDictionary = Optional.empty();
                return null;
            }

            if (lastInputDictionary == dictionary.get()) {
                // we must have fallen back last time if lastOutputDictionary is null
                return lastOutputDictionary.<Work<List<Block>>>map(CompletedWork::new).orElse(null);
            }

            // Process dictionary if:
            //   there is only one entry in the dictionary
            //   this is the first block
            //   the last dictionary was used for more positions than were in the dictionary
            boolean shouldProcessDictionary = dictionary.get().getPositionCount() == 1 || lastInputDictionary == null || lastDictionaryUsageCount >= lastInputDictionary.getPositionCount();

            // record the usage count regardless of dictionary processing choice, so we have stats for next time
            lastDictionaryUsageCount = 0;
            lastInputDictionary = dictionary.get();
            lastOutputDictionary = Optional.empty();

            if (shouldProcessDictionary) {
                return projection.project(properties, yieldSignal, new Page(lastInputDictionary), SelectedPositions.positionsRange(0, lastInputDictionary.getPositionCount()));
            }
            return null;
        }
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
