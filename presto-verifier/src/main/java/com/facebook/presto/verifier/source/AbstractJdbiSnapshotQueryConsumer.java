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
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public abstract class AbstractJdbiSnapshotQueryConsumer
        implements SnapshotQueryConsumer
{
    private final Provider<Jdbi> jdbiProvider;
    private final String tableName;

    public AbstractJdbiSnapshotQueryConsumer(Provider<Jdbi> jdbiProvider, String tableName)
    {
        this.jdbiProvider = requireNonNull(jdbiProvider, "jdbiProvider is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @Override
    public void accept(SnapshotQuery snapshot)
    {
        try (Handle handle = jdbiProvider.get().open()) {
            VerifierDao verifierDao = handle.attach(VerifierDao.class);
            verifierDao.saveSnapShot(tableName, snapshot.getSuite(), snapshot.getName(), snapshot.isExplain(), snapshot.getSnapshot());
        }
    }
}
