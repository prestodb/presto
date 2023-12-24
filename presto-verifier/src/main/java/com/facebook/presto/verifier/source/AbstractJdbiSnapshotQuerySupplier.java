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
package com.facebook.presto.verifier.source;

import com.facebook.presto.verifier.framework.SnapshotQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Provider;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractJdbiSnapshotQuerySupplier
        implements SnapshotQuerySupplier
{
    public static final String VERIFIER_SNAPSHOT_KEY_PATTERN = "SUITE_%s_@TEST_%s_@EXPLAIN_%s";
    private final Provider<Jdbi> jdbiProvider;
    private final String tableName;
    private final List<String> suites;
    private final int maxQueriesPerSuite;
    public AbstractJdbiSnapshotQuerySupplier(Provider<Jdbi> jdbiProvider, String tableName, List<String> suites, int maxQueriesPerSuite)
    {
        this.jdbiProvider = requireNonNull(jdbiProvider, "jdbiProvider is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.suites = ImmutableList.copyOf(suites);
        this.maxQueriesPerSuite = maxQueriesPerSuite;
    }

    @Override
    public Map<String, SnapshotQuery> get()
    {
        ImmutableMap.Builder<String, SnapshotQuery> snapshotQueriesMap = ImmutableMap.builder();
        try (Handle handle = jdbiProvider.get().open()) {
            VerifierDao verifierDao = handle.attach(VerifierDao.class);
            for (String suite : suites) {
                for (SnapshotQuery snapshotQuery : verifierDao.getSnapshotQueries(tableName, suite, maxQueriesPerSuite)) {
                    String key = format(VERIFIER_SNAPSHOT_KEY_PATTERN, snapshotQuery.getSuite(), snapshotQuery.getName(), snapshotQuery.isExplain());
                    snapshotQueriesMap.put(key, snapshotQuery);
                }
            }
        }
        return snapshotQueriesMap.build();
    }
}
