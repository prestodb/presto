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

import com.facebook.presto.verifier.framework.SourceQuery;
import com.google.common.collect.ImmutableList;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Provider;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class AbstractJdbiSourceQuerySupplier
        implements SourceQuerySupplier
{
    private final Provider<Jdbi> jdbiProvider;
    private final String tableName;
    private final List<String> suites;
    private final int maxQueriesPerSuite;

    public AbstractJdbiSourceQuerySupplier(Provider<Jdbi> jdbiProvider, String tableName, List<String> suites, int maxQueriesPerSuite)
    {
        this.jdbiProvider = requireNonNull(jdbiProvider, "jdbiProvider is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.suites = ImmutableList.copyOf(suites);
        this.maxQueriesPerSuite = maxQueriesPerSuite;
    }

    @Override
    public List<SourceQuery> get()
    {
        ImmutableList.Builder<SourceQuery> sourceQueries = ImmutableList.builder();
        try (Handle handle = jdbiProvider.get().open()) {
            VerifierDao verifierDao = handle.attach(VerifierDao.class);
            for (String suite : suites) {
                sourceQueries.addAll(verifierDao.getSourceQueries(tableName, suite, maxQueriesPerSuite));
            }
        }
        return sourceQueries.build();
    }
}
