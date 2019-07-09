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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.relation.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FilterFunction
{
    private final ConnectorSession session;
    private final Predicate predicate;
    private final boolean deterministic;
    private final int[] inputChannels;

    public FilterFunction(ConnectorSession session, boolean deterministic, Predicate predicate)
    {
        this.session = requireNonNull(session, "session is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.deterministic = deterministic;
        this.inputChannels = requireNonNull(predicate.getInputChannels(), "inputChannels is null");
    }

    /**
     * Evaluates specified positions and returns the number of positions that passed the filter.
     *
     * Upon return, the positions array will contain positions that passed the filter or encountered
     * an error. The errors array will contain errors that occurred while evaluating this filter as
     * well as errors that happened earlier. The latter will be preserved only for positions that
     * passed this filter.
     *
     * @param positions Monotonically increasing list of positions to evaluate
     * @param positionCount Number of valid entries in the positions array
     * @param errors Errors encountered while evaluating other filters, if any;
     *                  contains at least positionCount entries; null value indicates no error
     */
    public int filter(Page page, int[] positions, int positionCount, RuntimeException[] errors)
    {
        checkArgument(positionCount <= positions.length);
        checkArgument(positionCount <= errors.length);

        int outputCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            try {
                if (predicate.evaluate(session, page, position)) {
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

    public int[] getInputChannels()
    {
        return inputChannels;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
