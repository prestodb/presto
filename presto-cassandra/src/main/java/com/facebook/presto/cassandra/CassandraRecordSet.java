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
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.facebook.presto.cassandra.CassandraColumnHandle.cassandraFullTypeGetter;
import static com.facebook.presto.cassandra.CassandraColumnHandle.nativeTypeGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.transform;

public class CassandraRecordSet
        implements RecordSet
{
    private final String cql;
    private final List<FullCassandraType> cassandraTypes;
    private final List<Type> columnTypes;
    private final CassandraSession cassandraSession;

    public CassandraRecordSet(CassandraSession cassandraSession, String cql, List<CassandraColumnHandle> cassandraColumns)
    {
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.cql = checkNotNull(cql, "cql is null");
        checkNotNull(cassandraColumns, "cassandraColumns is null");
        this.cassandraTypes = transform(cassandraColumns, cassandraFullTypeGetter());
        this.columnTypes = transform(cassandraColumns, nativeTypeGetter());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new CassandraRecordCursor(cassandraSession, cassandraTypes, cql);
    }
}
