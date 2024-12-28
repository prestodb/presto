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
package com.facebook.presto.spi.function.scalar;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public enum ScalarFunctionStatsPropagationBehavior
{
    /** Use the max value across all arguments to derive the new stats value */
    USE_MAX_ARGUMENT,
    /** Use the min value across all arguments to derive the new stats value */
    USE_MIN_ARGUMENT,
    /** Sum the stats value of all arguments to derive the new stats value */
    SUM_ARGUMENTS,
    /** Propagate the source stats as-is */
    USE_SOURCE_STATS,
    // Following stats are independent of source stats.
    /** Use the value of output row count. */
    ROW_COUNT,
    /** Use the value of row_count * (1 - null_fraction). */
    NON_NULL_ROW_COUNT,
    /** use the value of TYPE_WIDTH in varchar(TYPE_WIDTH) */
    USE_TYPE_WIDTH_VARCHAR,
    /** Take max of type width of arguments with varchar type. */
    MAX_TYPE_WIDTH_VARCHAR,
    /** Stats are unknown and thus no action is performed. */
    UNKNOWN;
    /*
These behavior's utilize stats from more than one argument of the scalar function to compute new output stats
     */
    private static final Set<ScalarFunctionStatsPropagationBehavior> MULTI_ARGUMENT_STATS =
            unmodifiableSet(new HashSet<>(Arrays.asList(MAX_TYPE_WIDTH_VARCHAR, USE_MAX_ARGUMENT, USE_MIN_ARGUMENT, SUM_ARGUMENTS)));
    private static final Set<ScalarFunctionStatsPropagationBehavior> SOURCE_STATS_DEPENDENT_STATS =
            unmodifiableSet(new HashSet<>(Arrays.asList(USE_MAX_ARGUMENT, USE_MIN_ARGUMENT, SUM_ARGUMENTS, USE_SOURCE_STATS)));

    public boolean isMultiArgumentStat()
    {
        return MULTI_ARGUMENT_STATS.contains(this);
    }

    public boolean isSourceStatsDependentStats()
    {
        return SOURCE_STATS_DEPENDENT_STATS.contains(this);
    }
}
