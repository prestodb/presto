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

import org.jdbi.v3.core.mapper.reflect.ColumnName;
import org.jdbi.v3.core.mapper.reflect.JdbiConstructor;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SnapshotQuery
{
    private final String suite;
    private final String name;
    private final boolean explain;
    private final String snapshot;

    @JdbiConstructor
    public SnapshotQuery(
            @ColumnName("suite") String suite,
            @ColumnName("name") String name,
            @ColumnName("is_explain") boolean explain,
            @ColumnName("snapshot") String snapshot)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.name = requireNonNull(name, "name is null");
        this.explain = requireNonNull(explain, "explain is null");
        this.snapshot = requireNonNull(snapshot, "snapshot is null");
    }

    public String getSuite()
    {
        return suite;
    }

    public String getName()
    {
        return name;
    }

    public boolean isExplain()
    {
        return explain;
    }

    public String getSnapshot()
    {
        return snapshot;
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
        SnapshotQuery o = (SnapshotQuery) obj;
        return Objects.equals(suite, o.suite) &&
                Objects.equals(name, o.name) &&
                Objects.equals(snapshot, o.snapshot);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(suite, name, snapshot);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("suite", suite)
                .add("name", name)
                .add("snapshot", snapshot)
                .toString();
    }
}
