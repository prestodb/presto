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
package com.facebook.presto.verifier.framework;

import org.jdbi.v3.core.mapper.Nested;
import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SourceQuery
{
    private final String suite;
    private final String name;
    private final String controlQuery;
    private final String testQuery;
    private final Optional<String> controlQueryId;
    private final Optional<String> testQueryId;
    private final QueryConfiguration controlConfiguration;
    private final QueryConfiguration testConfiguration;

    public SourceQuery(
            String suite,
            String name,
            String controlQuery,
            String testQuery,
            Optional<String> controlQueryId,
            Optional<String> testQueryId,
            QueryConfiguration controlConfiguration,
            QueryConfiguration testConfiguration)
    {
        this(suite, name, controlQuery, null, testQuery, null, controlQueryId, testQueryId, controlConfiguration, testConfiguration);
    }

    @JdbiConstructor
    public SourceQuery(
            @ColumnName("suite") String suite,
            @ColumnName("name") String name,
            @ColumnName("controlQuery") String controlQuery,
            @ColumnName("controlQueryUtf8") String controlQueryUtf8,
            @ColumnName("testQuery") String testQuery,
            @ColumnName("testQueryUtf8") String testQueryUtf8,
            @ColumnName("controlQueryId") Optional<String> controlQueryId,
            @ColumnName("testQueryId") Optional<String> testQueryId,
            @Nested("control") QueryConfiguration controlConfiguration,
            @Nested("test") QueryConfiguration testConfiguration)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.name = requireNonNull(name, "name is null");
        this.controlQuery = clean((controlQueryUtf8 == null || controlQueryUtf8.trim().isEmpty()) ? controlQuery : controlQueryUtf8);
        this.testQuery = clean((testQueryUtf8 == null || testQueryUtf8.trim().isEmpty()) ? testQuery : testQueryUtf8);
        this.controlQueryId = requireNonNull(controlQueryId, "controlQueryId is null");
        this.testQueryId = requireNonNull(testQueryId, "testQueryId is null");
        this.controlConfiguration = requireNonNull(controlConfiguration, "controlConfiguration is null");
        this.testConfiguration = requireNonNull(testConfiguration, "testConfiguration is null");
    }

    public String getSuite()
    {
        return suite;
    }

    public String getName()
    {
        return name;
    }

    public String getQuery(ClusterType clusterType)
    {
        checkArgument(clusterType == CONTROL || clusterType == TEST, "Invalid ClusterType: %s", clusterType);
        return clusterType == CONTROL ? controlQuery : testQuery;
    }

    public Optional<String> getQueryId(ClusterType clusterType)
    {
        checkArgument(clusterType == CONTROL || clusterType == TEST, "Invalid ClusterType: %s", clusterType);
        return clusterType == CONTROL ? controlQueryId : testQueryId;
    }

    public QueryConfiguration getQueryConfiguration(ClusterType clusterType)
    {
        checkArgument(clusterType == CONTROL || clusterType == TEST, "Invalid ClusterType: %s", clusterType);
        return clusterType == CONTROL ? controlConfiguration : testConfiguration;
    }

    public QueryConfiguration getControlConfiguration()
    {
        return controlConfiguration;
    }

    public QueryConfiguration getTestConfiguration()
    {
        return testConfiguration;
    }

    private static String clean(String sql)
    {
        sql = sql.replaceAll("\t", "  ");
        sql = sql.replaceAll("\n+", "\n");
        sql = sql.trim();
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        SourceQuery o = (SourceQuery) obj;
        return Objects.equals(suite, o.suite) &&
                Objects.equals(name, o.name) &&
                Objects.equals(controlQuery, o.controlQuery) &&
                Objects.equals(testQuery, o.testQuery) &&
                Objects.equals(controlQueryId, o.controlQueryId) &&
                Objects.equals(testQueryId, o.testQueryId) &&
                Objects.equals(controlConfiguration, o.controlConfiguration) &&
                Objects.equals(testConfiguration, o.testConfiguration);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(suite, name, controlQuery, testQuery, controlQueryId, testQueryId, controlConfiguration, testConfiguration);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("suite", suite)
                .add("name", name)
                .add("controlQuery", controlQuery)
                .add("testQuery", testQuery)
                .add("controlQueryId", controlQueryId)
                .add("testQueryId", testQueryId)
                .add("controlConfiguration", controlConfiguration)
                .add("testConfiguration", testConfiguration)
                .toString();
    }
}
