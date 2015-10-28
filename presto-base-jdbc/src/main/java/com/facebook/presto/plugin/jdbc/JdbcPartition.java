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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.predicate.TupleDomain;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JdbcPartition
        implements ConnectorPartition
{
    private final JdbcTableHandle jdbcTableHandle;
    private final TupleDomain<ColumnHandle> domain;

    public JdbcPartition(JdbcTableHandle jdbcTableHandle, TupleDomain<ColumnHandle> domain)
    {
        this.jdbcTableHandle = requireNonNull(jdbcTableHandle, "jdbcTableHandle is null");
        this.domain = requireNonNull(domain, "domain is null");
    }

    @Override
    public String getPartitionId()
    {
        return jdbcTableHandle.toString();
    }

    public JdbcTableHandle getJdbcTableHandle()
    {
        return jdbcTableHandle;
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return domain;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("jdbcTableHandle", jdbcTableHandle)
                .toString();
    }
}
