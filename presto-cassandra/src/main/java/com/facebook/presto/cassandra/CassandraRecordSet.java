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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.transform;

import java.util.List;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;

public class CassandraRecordSet implements RecordSet {

	private final String cql;
	private final List<FullCassandraType> cassandraTypes;
	private final List<ColumnType> columnTypes;
	private final CassandraSession cassandraSession;

	public CassandraRecordSet(String cql, List<CassandraColumnHandle> cassandraColumns,
			CassandraSession cassandraSession) {
		checkNotNull(cql, "CQL query must not be null");
		checkNotNull(cassandraSession, "cassandraSession must not be null");
		this.cql = cql;
		this.cassandraTypes = transform(cassandraColumns,
				CassandraColumnHandle.cassandraFullTypeGetter());
		this.columnTypes = transform(cassandraColumns, CassandraColumnHandle.nativeTypeGetter());
		this.cassandraSession = cassandraSession;
	}

	@Override
	public List<ColumnType> getColumnTypes() {
		return columnTypes;
	}

	@Override
	public RecordCursor cursor() {
		return new CassandraRecordCursor(cassandraSession, cassandraTypes, cql);
	}
}
