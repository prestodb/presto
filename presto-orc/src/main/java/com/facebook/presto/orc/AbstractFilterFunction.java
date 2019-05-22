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

import com.facebook.presto.spi.Page;

public abstract class AbstractFilterFunction
        extends FilterStats
{
    protected final int[] inputChannels;
    protected int initialCost = 1;
    private int[][] channelRowNumberMaps;

    public AbstractFilterFunction(int[] inputChannels, int initialCost)
    {
        this.inputChannels = inputChannels;
        this.channelRowNumberMaps = new int[inputChannels.length][];
        this.initialCost = initialCost;
    }

    public int[] getInputChannels()
    {
        return inputChannels;
    }

    public abstract boolean isDeterministic();

    /* Sets outputRows to be the list of positions on page for
     * which the filter is true. Returns the number of positions
     * written to outputRows. outputRows is expected to have at
     * least page.getPositionCount() elements. If errorSet is non
     * null, exceptions are caught and returned in errorSet. These
     * correspond pairwise to the row numbers in rows. A row that
     * produces an error is considered as included in the
     * output. */
    public abstract int filter(Page page, int[] outputRows, ErrorSet errorSet);

    public int[][] getChannelRowNumberMaps()
    {
        return channelRowNumberMaps;
    }
}
