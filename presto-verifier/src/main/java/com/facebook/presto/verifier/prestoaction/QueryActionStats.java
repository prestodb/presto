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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.jdbc.QueryStats;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryActionStats
{
    public static final QueryActionStats EMPTY_STATS = new QueryActionStats(Optional.empty(), Optional.empty());

    private final Optional<QueryStats> queryStats;
    private final Optional<String> extraStats;

    public QueryActionStats(Optional<QueryStats> queryStats, Optional<String> extraStats)
    {
        this.queryStats = requireNonNull(queryStats, "queryStats is null");
        this.extraStats = requireNonNull(extraStats, "extraStats is null");
    }

    public Optional<QueryStats> getQueryStats()
    {
        return queryStats;
    }

    public Optional<String> getExtraStats()
    {
        return extraStats;
    }
}
