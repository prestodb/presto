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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.UnaryOperator.identity;

public class BenchmarkSuite
{
    private final String suite;
    private final String querySet;
    private final List<PhaseSpecification> phases;
    private final Map<String, String> sessionProperties;
    private final Map<String, BenchmarkQuery> queries;

    public BenchmarkSuite(
            String suite,
            String querySet,
            List<PhaseSpecification> phases,
            Map<String, String> sessionProperties,
            List<BenchmarkQuery> queries)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.querySet = requireNonNull(querySet, "querySet is null");
        this.phases = requireNonNull(phases, "phases is null");
        this.sessionProperties = ImmutableMap.copyOf(sessionProperties);
        this.queries = queries.stream()
                .collect(toImmutableMap(BenchmarkQuery::getName, identity()));
    }

    public String getSuite()
    {
        return suite;
    }

    public String getQuerySet()
    {
        return querySet;
    }

    public List<PhaseSpecification> getPhases()
    {
        return phases;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    public Map<String, BenchmarkQuery> getQueries()
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
        return Objects.equals(suite, o.suite) &&
                Objects.equals(querySet, o.querySet) &&
                Objects.equals(phases, o.phases) &&
                Objects.equals(sessionProperties, o.sessionProperties) &&
                Objects.equals(queries, o.queries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(suite, querySet, phases, sessionProperties, queries);
    }

    public static class JdbiBuilder
    {
        private final String suite;
        private final String querySet;
        private final List<PhaseSpecification> phases;
        private final Map<String, String> sessionProperties;
        private List<BenchmarkQuery> queries;

        @JdbiConstructor
        public JdbiBuilder(
                @ColumnName("suite") String suite,
                @ColumnName("query_set") String querySet,
                @ColumnName("phases") List<PhaseSpecification> phases,
                @ColumnName("session_properties") Optional<Map<String, String>> sessionProperties)
        {
            this.suite = requireNonNull(suite, "suite is null");
            this.querySet = requireNonNull(querySet, "querySet is null");
            this.phases = requireNonNull(phases, "phases is null");
            this.sessionProperties = sessionProperties.orElse(ImmutableMap.of());
        }

        public String getQuerySet()
        {
            return querySet;
        }

        public JdbiBuilder setQueries(List<BenchmarkQuery> queries)
        {
            this.queries = ImmutableList.copyOf(queries);
            return this;
        }

        public BenchmarkSuite build()
        {
            return new BenchmarkSuite(suite, querySet, phases, sessionProperties, queries);
        }
    }
}
