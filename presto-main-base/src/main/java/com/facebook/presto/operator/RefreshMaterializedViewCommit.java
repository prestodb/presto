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
package com.facebook.presto.operator;

import io.airlift.slice.Slice;

import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * The two fragment sets an atomic materialized view refresh commits together: the rows to remove
 * from the storage table and the rows to add. Committing them in one operation is what keeps a
 * refresh from exposing a state where affected rows are missing.
 */
public final class RefreshMaterializedViewCommit
{
    private final Collection<Slice> deleteFragments;
    private final Collection<Slice> insertFragments;

    public RefreshMaterializedViewCommit(Collection<Slice> deleteFragments, Collection<Slice> insertFragments)
    {
        this.deleteFragments = requireNonNull(deleteFragments, "deleteFragments is null");
        this.insertFragments = requireNonNull(insertFragments, "insertFragments is null");
    }

    public Collection<Slice> getDeleteFragments()
    {
        return deleteFragments;
    }

    public Collection<Slice> getInsertFragments()
    {
        return insertFragments;
    }
}
