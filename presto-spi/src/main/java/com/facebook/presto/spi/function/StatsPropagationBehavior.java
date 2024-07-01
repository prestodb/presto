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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum StatsPropagationBehavior
{
    USE_MAX_ARGUMENT,
    SUM_ARGUMENTS,
    USE_SOURCE_STATS,
    ROW_COUNT,
    ROW_COUNT_TIMES_INV_NULL_FRACTION, // row_count * (1 - null_fraction)
    USE_TYPE_WIDTH_VARCHAR,
    MAX_TYPE_WIDTH_VARCHAR,
    UNKNOWN;

    /*
     * Stats are single argument when their value is calculated by operating on stats from source stats of the argument which
     * is annotated.
     */
    public static final Set<StatsPropagationBehavior> SINGLE_ARGUMENT_STATS =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(UNKNOWN, ROW_COUNT, ROW_COUNT_TIMES_INV_NULL_FRACTION, USE_TYPE_WIDTH_VARCHAR, USE_SOURCE_STATS)));

    public boolean isSingleArgumentStats()
    {
        return SINGLE_ARGUMENT_STATS.contains(this);
    }
}
