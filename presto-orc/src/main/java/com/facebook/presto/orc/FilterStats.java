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

public class FilterStats
{
    protected long nIn;
    protected long nOut;
    protected long time;

    public void updateStats(int nIn, int nOut, long time)
    {
        this.nIn += nIn;
        this.nOut += nOut;
        this.time += time;
    }

    public double getTimePerDroppedValue()
    {
        return (double) time / (1 + nIn - nOut);
    }

    public double getSelectivity()
    {
        if (nIn == 0) {
            return 1;
        }
        return (double) nOut / (double) nIn;
    }

    public void decayStats()
    {
        nIn /= 2;
        nOut /= 2;
        time /= 2;
    }
}
