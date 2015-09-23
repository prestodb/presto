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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.systemtables.ShardMetadataSystemTable;
import com.facebook.presto.spi.SystemTable;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Provider;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RaptorSystemTablesFactory
        implements Provider<Set<SystemTable>>
{
    private final IDBI dbi;

    @Inject
    public RaptorSystemTablesFactory(@ForMetadata IDBI dbi)
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
    }

    public Set<SystemTable> get()
    {
        return ImmutableSet.of(new ShardMetadataSystemTable(dbi));
    }
}
