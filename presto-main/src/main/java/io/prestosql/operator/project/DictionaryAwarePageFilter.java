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
package io.prestosql.operator.project;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DictionaryAwarePageFilter
        implements PageFilter
{
    private final PageFilter filter;

    private Block lastInputDictionary;
    private Optional<boolean[]> lastOutputDictionary;
    private long lastDictionaryUsageCount;

    public DictionaryAwarePageFilter(PageFilter filter)
    {
        this.filter = requireNonNull(filter, "filter is null");

        verify(filter.isDeterministic(), "filter must be deterministic");
        verify(filter.getInputChannels().size() == 1, "filter must have only one input");
    }

    @Override
    public boolean isDeterministic()
    {
        return filter.isDeterministic();
    }

    @Override
    public InputChannels getInputChannels()
    {
        return filter.getInputChannels();
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        Block block = page.getBlock(0).getLoadedBlock();

        if (block instanceof RunLengthEncodedBlock) {
            Block value = ((RunLengthEncodedBlock) block).getValue();
            Optional<boolean[]> selectedPosition = processDictionary(session, value);
            // single value block is always considered effective, but the processing could have thrown
            // in that case we fallback and process again so the correct error message sent
            if (selectedPosition.isPresent()) {
                return SelectedPositions.positionsRange(0, selectedPosition.get()[0] ? page.getPositionCount() : 0);
            }
        }

        if (block instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
            // Attempt to process the dictionary.  If dictionary is processing has not been considered effective, an empty response will be returned
            Optional<boolean[]> selectedDictionaryPositions = processDictionary(session, dictionaryBlock.getDictionary());
            // record the usage count regardless of dictionary processing choice, so we have stats for next time
            lastDictionaryUsageCount += page.getPositionCount();
            // if dictionary was processed, produce a dictionary block; otherwise do normal processing
            if (selectedDictionaryPositions.isPresent()) {
                return selectDictionaryPositions(dictionaryBlock, selectedDictionaryPositions.get());
            }
        }

        return filter.filter(session, new Page(block));
    }

    private Optional<boolean[]> processDictionary(ConnectorSession session, Block dictionary)
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
                SelectedPositions selectedDictionaryPositions = filter.filter(session, new Page(dictionary));
                lastOutputDictionary = Optional.of(toPositionsMask(selectedDictionaryPositions, dictionary.getPositionCount()));
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

    private static SelectedPositions selectDictionaryPositions(DictionaryBlock dictionaryBlock, boolean[] selectedDictionaryPositions)
    {
        int selectedCount = 0;
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            if (selectedDictionaryPositions[dictionaryBlock.getId(position)]) {
                selectedCount++;
            }
        }

        if (selectedCount == 0 || selectedCount == dictionaryBlock.getPositionCount()) {
            return SelectedPositions.positionsRange(0, selectedCount);
        }

        int[] positions = new int[selectedCount];
        int index = 0;
        for (int position = 0; position < dictionaryBlock.getPositionCount(); position++) {
            if (selectedDictionaryPositions[dictionaryBlock.getId(position)]) {
                positions[index] = position;
                index++;
            }
        }
        return SelectedPositions.positionsList(positions, 0, selectedCount);
    }

    private static boolean[] toPositionsMask(SelectedPositions selectedPositions, int positionCount)
    {
        boolean[] positionsMask = new boolean[positionCount];
        if (selectedPositions.isList()) {
            int offset = selectedPositions.getOffset();
            int[] positions = selectedPositions.getPositions();
            for (int index = offset; index < offset + selectedPositions.size(); index++) {
                positionsMask[positions[index]] = true;
            }
        }
        else {
            int offset = selectedPositions.getOffset();
            for (int position = offset; position < offset + selectedPositions.size(); position++) {
                positionsMask[position] = true;
            }
        }
        return positionsMask;
    }
}
