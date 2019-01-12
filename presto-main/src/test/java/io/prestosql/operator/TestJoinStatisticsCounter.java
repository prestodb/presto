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
package io.prestosql.operator;

import io.prestosql.operator.LookupJoinOperators.JoinType;
import org.testng.annotations.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

public class TestJoinStatisticsCounter
{
    @Test
    public void testRecord()
    {
        JoinStatisticsCounter counter = new JoinStatisticsCounter(JoinType.INNER);
        JoinOperatorInfo info = counter.get();
        assertEquals(makeHistogramArray(0, 0, 0, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 0, 0, 0, 0, 0, 0, 0), info.getLogHistogramOutput());

        // 0 to 4 buckets
        counter.recordProbe(0);
        info = counter.get();
        assertEquals(makeHistogramArray(1, 0, 0, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 0, 0, 0, 0, 0, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(0);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 0, 0, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 0, 0, 0, 0, 0, 0, 0), info.getLogHistogramOutput());

        counter.recordProbe(1);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 1, 0, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 1, 0, 0, 0, 0, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(1);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 0, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 0, 0, 0, 0, 0, 0), info.getLogHistogramOutput());

        counter.recordProbe(2);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 1, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 2, 0, 0, 0, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(2);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 0, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 0, 0, 0, 0, 0), info.getLogHistogramOutput());

        counter.recordProbe(3);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 1, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 3, 0, 0, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(3);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 0, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 0, 0, 0, 0), info.getLogHistogramOutput());

        counter.recordProbe(4);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 1, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 4, 0, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(4);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 0, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 0, 0, 0), info.getLogHistogramOutput());

        // 5 to 10
        counter.recordProbe(5);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 1, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 5, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(6);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 2, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 11, 0, 0), info.getLogHistogramOutput());
        counter.recordProbe(10);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 3, 0, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 21, 0, 0), info.getLogHistogramOutput());

        // 11 to 100
        counter.recordProbe(11);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 3, 1, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 21, 11, 0), info.getLogHistogramOutput());
        counter.recordProbe(100);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 3, 2, 0), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 21, 111, 0), info.getLogHistogramOutput());

        // 101 and more
        counter.recordProbe(101);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 3, 2, 1), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 21, 111, 101), info.getLogHistogramOutput());
        counter.recordProbe(1000);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 3, 2, 2), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 21, 111, 1101), info.getLogHistogramOutput());
        counter.recordProbe(1000000);
        info = counter.get();
        assertEquals(makeHistogramArray(2, 2, 2, 2, 2, 3, 2, 3), info.getLogHistogramProbes());
        assertEquals(makeHistogramArray(0, 2, 4, 6, 8, 21, 111, 1001101), info.getLogHistogramOutput());
    }

    private long[] makeHistogramArray(long... longArray)
    {
        checkArgument(longArray.length == 8);
        return longArray;
    }
}
