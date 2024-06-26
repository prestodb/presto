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
package com.facebook.presto.spi.function;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum PropagateSourceStats
{
    MAX,
    SUM,
    SOURCE_STATS,
    ROW_COUNT,
    ROW_COUNT_TIMES_INV_NULL_FRACTION, // row_count * (1 - null_fraction)
    TYPE_WIDTH_VARCHAR,
    MAX_TYPE_WIDTH_VARCHAR,
    UNKNOWN;

    /*
     * Stats are simple when their value is calculated by operating on stats from source stats of the argument which
     * is annotated.
     */
    public static boolean isSimpleStatsFunction(PropagateSourceStats op)
    {
        Set<PropagateSourceStats> s = new HashSet<>(Arrays.asList(UNKNOWN, ROW_COUNT, ROW_COUNT_TIMES_INV_NULL_FRACTION, TYPE_WIDTH_VARCHAR, SOURCE_STATS));
        return s.contains(op);
    }
}
