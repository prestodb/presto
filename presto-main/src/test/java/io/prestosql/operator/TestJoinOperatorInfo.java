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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

public class TestJoinOperatorInfo
{
    @Test
    public void testMerge()
    {
        JoinOperatorInfo base = new JoinOperatorInfo(
                JoinType.INNER,
                makeHistogramArray(10, 20, 30, 40, 50, 60, 70, 80),
                makeHistogramArray(12, 22, 32, 42, 52, 62, 72, 82),
                Optional.of(1L));
        JoinOperatorInfo other = new JoinOperatorInfo(
                JoinType.INNER,
                makeHistogramArray(11, 21, 31, 41, 51, 61, 71, 81),
                makeHistogramArray(15, 25, 35, 45, 55, 65, 75, 85),
                Optional.of(2L));

        JoinOperatorInfo merged = base.mergeWith(other);
        assertEquals(makeHistogramArray(21, 41, 61, 81, 101, 121, 141, 161), merged.getLogHistogramProbes());
        assertEquals(makeHistogramArray(27, 47, 67, 87, 107, 127, 147, 167), merged.getLogHistogramOutput());
        assertEquals(merged.getLookupSourcePositions(), Optional.of(3L));
    }

    private long[] makeHistogramArray(long... longArray)
    {
        checkArgument(longArray.length == 8);
        return longArray;
    }
}
