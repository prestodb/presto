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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.relation.Predicate;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class FilterFunction
{
    private static final byte FILTER_NOT_EVALUATED = 0;
    private static final byte FILTER_PASSED = 1;
    private static final byte FILTER_FAILED = 2;

    private final SqlFunctionProperties properties;
    private final Predicate predicate;
    private final boolean deterministic;
    private final int[] inputChannels;

    // If the function has a single argument and this is a DictionaryBlock, we can cache results. The cache is valid as long
    // as the dictionary inside the block is physically the same.
    private byte[] dictionaryResults;
    private Block previousDictionary;
    private Page dictionaryPage;

    public FilterFunction(SqlFunctionProperties properties, boolean deterministic, Predicate predicate)
    {
        this.properties = requireNonNull(properties, "properties is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.deterministic = deterministic;
        this.inputChannels = requireNonNull(predicate.getInputChannels(), "inputChannels is null");
    }

    /**
     * Evaluates specified positions and returns the number of positions that passed the filter.
     * <p>
     * Upon return, the positions array will contain positions that passed the filter or encountered
     * an error. The errors array will contain errors that occurred while evaluating this filter as
     * well as errors that happened earlier. The latter will be preserved only for positions that
     * passed this filter.
     *
     * @param positions Monotonically increasing list of positions to evaluate
     * @param positionCount Number of valid entries in the positions array
     * @param errors Errors encountered while evaluating other filters, if any;
     * contains at least positionCount entries; null value indicates no error
     */
    public int filter(Page page, int[] positions, int positionCount, RuntimeException[] errors)
    {
        checkArgument(positionCount <= positions.length);
        checkArgument(positionCount <= errors.length);

        if (deterministic && inputChannels.length == 1 && page.getBlock(0) instanceof DictionaryBlock) {
            return filterWithDictionary(page, positions, positionCount, errors);
        }

        int outputCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            try {
                if (predicate.evaluate(properties, page, position)) {
                    positions[outputCount] = position;
                    errors[outputCount] = errors[i];
                    outputCount++;
                }
            }
            catch (RuntimeException e) {
                positions[outputCount] = position;
                errors[outputCount] = e;    // keep last error
                outputCount++;
            }
        }

        return outputCount;
    }

    private int filterWithDictionary(Page page, int[] positions, int positionCount, RuntimeException[] errors)
    {
        int outputCount = 0;
        DictionaryBlock block = (DictionaryBlock) page.getBlock(0);
        Block dictionary = block.getDictionary();
        if (dictionary != previousDictionary) {
            previousDictionary = dictionary;
            int numEntries = dictionary.getPositionCount();
            dictionaryPage = new Page(numEntries, dictionary);
            dictionaryResults = ensureCapacity(dictionaryResults, numEntries);
            fill(dictionaryResults, 0, numEntries, FILTER_NOT_EVALUATED);
        }
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int dictionaryPosition = block.getId(position);
            byte result = dictionaryResults[dictionaryPosition];
            switch (result) {
                case FILTER_FAILED:
                    continue;
                case FILTER_PASSED:
                    positions[outputCount] = position;
                    errors[outputCount] = errors[i];
                    outputCount++;
                    continue;
                case FILTER_NOT_EVALUATED:
                    try {
                        if (predicate.evaluate(properties, dictionaryPage, dictionaryPosition)) {
                            positions[outputCount] = position;
                            errors[outputCount] = errors[i];
                            outputCount++;
                            dictionaryResults[dictionaryPosition] = FILTER_PASSED;
                        }
                        else {
                            dictionaryResults[dictionaryPosition] = FILTER_FAILED;
                        }
                    }
                    catch (RuntimeException e) {
                        // We do not record errors in the dictionary results.
                        positions[outputCount] = position;
                        errors[outputCount] = e;    // keep last error
                        outputCount++;
                    }
                    break;
                default:
                    verify(false, "Unexpected filter result: " + result);
            }
        }
        return outputCount;
    }

    public int[] getInputChannels()
    {
        return inputChannels;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
