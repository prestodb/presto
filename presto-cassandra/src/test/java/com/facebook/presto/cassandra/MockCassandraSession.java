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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

public class MockCassandraSession extends CassandraSession {
	static final String TEST_SCHEMA = "testkeyspace";
	static final String BAD_SCHEMA = "badkeyspace";
	static final String TEST_TABLE = "testtbl";
	static final String TEST_COLUMN1 = "column1";
	static final String TEST_COLUMN2 = "column2";
	static final String TEST_PARTITION_KEY1 = "testpartition1";
	static final String TEST_PARTITION_KEY2 = "testpartition2";

	private final AtomicInteger accessCount = new AtomicInteger();
	private boolean throwException;
	
	public MockCassandraSession(String connectorId) {
		super(connectorId, null, new CassandraClientConfig());
	}

	public void setThrowException(boolean throwException) {
		this.throwException = throwException;
	}

	public int getAccessCount() {
		return accessCount.get();
	}

	@Override
	public List<String> getAllSchemas() {
		accessCount.incrementAndGet();
		if (throwException) {
			throw new IllegalStateException();
		}
		return ImmutableList.of(TEST_SCHEMA);
	}

	@Override
	public List<String> getAllTables(String schema) throws NoSuchObjectException {
		accessCount.incrementAndGet();
		if (throwException) {
			throw new IllegalStateException();
		}
		if (schema.equals(TEST_SCHEMA)) {
			return ImmutableList.of(TEST_TABLE);
		} else {
			throw new NoSuchObjectException();
		}
	}

	@Override
	public void getSchema(String schema) throws NoSuchObjectException {
		accessCount.incrementAndGet();
		if (throwException) {
			throw new IllegalStateException();
		}
		if (!schema.equals(TEST_SCHEMA)) {
			throw new NoSuchObjectException();
		}
	}

	@Override
	public CassandraTable getTable(SchemaTableName tableName) throws NoSuchObjectException {
		accessCount.incrementAndGet();
		if (throwException) {
			throw new IllegalStateException();
		}
		if (tableName.getSchemaName().equals(TEST_SCHEMA)
				&& tableName.getTableName().equals(TEST_TABLE)) {
			CassandraColumnHandle column1 = new CassandraColumnHandle(connectorId, TEST_COLUMN1, 0, CassandraType.VARCHAR, null, true, false);
			CassandraColumnHandle column2 = new CassandraColumnHandle(connectorId, TEST_COLUMN2, 0, CassandraType.INT, null, false, false);
			CassandraTableHandle tableHandle = new CassandraTableHandle(connectorId, TEST_SCHEMA, TEST_TABLE);
			CassandraTable table = new CassandraTable(tableHandle, ImmutableList.of(column1, column2));
			return table;
		} else {
			throw new NoSuchObjectException();
		}
	}

	@Override
	public List<CassandraPartition> getPartitions(CassandraTable table, List<Comparable<?>> filterPrefix)
			throws NoSuchObjectException {
		accessCount.incrementAndGet();
		if (throwException) {
			throw new IllegalStateException();
		}
		
		return super.getPartitions(table, filterPrefix);
	}

	@Override
	protected List<Row> queryPartitionKeys(CassandraTable table, List<Comparable<?>> filterPrefix) {
		CassandraTableHandle tableHandle = table.getTableHandle();
		if (tableHandle.getSchemaName().equals(TEST_SCHEMA)
				&& tableHandle.getTableName().equals(TEST_TABLE)) {
			Row row1 = new TestRow(TEST_PARTITION_KEY1);
			Row row2 = new TestRow(TEST_PARTITION_KEY2);
			return ImmutableList.of(row1, row2);
		} else {
			throw new IllegalStateException();
		}
	}

	@Override
	public Set<Host> getReplicas(String schema, ByteBuffer partitionKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ResultSet executeQuery(String cql) {
		throw new IllegalStateException("unexpected CQL query: " + cql);
	}

	private static class TestRow implements Row {
		private final String column0;
		
		TestRow(String column0) {
			this.column0 = column0;
		}
		
		@Override
		public ColumnDefinitions getColumnDefinitions() {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isNull(int i) {
			return false;
		}

		@Override
		public boolean isNull(String name) {
			return false;
		}

		@Override
		public boolean getBool(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public boolean getBool(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public int getInt(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public int getInt(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public long getLong(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public long getLong(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public Date getDate(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public Date getDate(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public float getFloat(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public float getFloat(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public double getDouble(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public double getDouble(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public ByteBuffer getBytesUnsafe(int i) {
			if (i == 0) {
				return ByteBuffer.wrap(column0.getBytes(Charsets.UTF_8));
			}
			throw new IndexOutOfBoundsException();
		}

		@Override
		public ByteBuffer getBytesUnsafe(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public ByteBuffer getBytes(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public ByteBuffer getBytes(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public String getString(int i) {
			if (i == 0) {
				return column0;
			}
			throw new InvalidTypeException("");
		}

		@Override
		public String getString(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public BigInteger getVarint(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public BigInteger getVarint(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public BigDecimal getDecimal(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public BigDecimal getDecimal(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public UUID getUUID(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public UUID getUUID(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public InetAddress getInet(int i) {
			throw new InvalidTypeException("");
		}

		@Override
		public InetAddress getInet(String name) {
			throw new InvalidTypeException("");
		}

		@Override
		public <T> List<T> getList(int i, Class<T> elementsClass) {
			throw new InvalidTypeException("");
		}

		@Override
		public <T> List<T> getList(String name, Class<T> elementsClass) {
			throw new InvalidTypeException("");
		}

		@Override
		public <T> Set<T> getSet(int i, Class<T> elementsClass) {
			throw new InvalidTypeException("");
		}

		@Override
		public <T> Set<T> getSet(String name, Class<T> elementsClass) {
			throw new InvalidTypeException("");
		}

		@Override
		public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
			throw new InvalidTypeException("");
		}

		@Override
		public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
			throw new InvalidTypeException("");
		}
		
	}
}
