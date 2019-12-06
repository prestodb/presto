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
package com.facebook.presto.benchmark.framework;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class BenchmarkSuite
{
    private final String name;
    private final BenchmarkSuiteInfo suiteInfo;
    private final Map<String, BenchmarkQuery> queries;

    public BenchmarkSuite(
            String name,
            BenchmarkSuiteInfo suiteInfo,
            List<BenchmarkQuery> queries)
    {
        this.name = requireNonNull(name, "name is null");
        this.suiteInfo = requireNonNull(suiteInfo, "SuiteInfo is null");
        this.queries = queries.stream()
                .collect(toImmutableMap(BenchmarkQuery::getName, identity()));
    }

    public String getName()
    {
        return name;
    }

    public BenchmarkSuiteInfo getSuiteInfo()
    {
        return suiteInfo;
    }

    public Map<String, BenchmarkQuery> getQueryMap()
    {
        return queries;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BenchmarkSuite o = (BenchmarkSuite) obj;
        return Objects.equals(suiteInfo, o.suiteInfo) &&
                Objects.equals(queries, o.queries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(suiteInfo, queries);
    }
}
