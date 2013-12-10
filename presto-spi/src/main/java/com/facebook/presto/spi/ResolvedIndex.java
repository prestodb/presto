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
package com.facebook.presto.spi;

import java.util.Objects;

public class ResolvedIndex
{
    private final IndexHandle indexHandle;
    private final TupleDomain unresolvedTupleDomain;

    public ResolvedIndex(IndexHandle indexHandle, TupleDomain unresolvedTupleDomain)
    {
        this.indexHandle = Objects.requireNonNull(indexHandle, "indexHandle is null");
        this.unresolvedTupleDomain = Objects.requireNonNull(unresolvedTupleDomain, "unresolvedTupleDomain is null");
    }

    public IndexHandle getIndexHandle()
    {
        return indexHandle;
    }

    public TupleDomain getUnresolvedTupleDomain()
    {
        return unresolvedTupleDomain;
    }
}
