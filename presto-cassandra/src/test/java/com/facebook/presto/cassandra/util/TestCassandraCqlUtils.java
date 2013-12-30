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
package com.facebook.presto.cassandra.util;

import static org.testng.Assert.assertEquals;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.testng.annotations.Test;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.TestHost;
import com.facebook.presto.cassandra.CassandraColumnHandle;
import com.facebook.presto.cassandra.CassandraType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.HostAddress;

public class TestCassandraCqlUtils {
	@Test
	public void testValidSchemaName() {
		assertEquals("foo", CassandraCqlUtils.validSchemaName("foo"));
		assertEquals("\"select\"", CassandraCqlUtils.validSchemaName("select"));
	}

	@Test
	public void testValidTableName() {
		assertEquals("foo", CassandraCqlUtils.validTableName("foo"));
		assertEquals("\"Foo\"", CassandraCqlUtils.validTableName("Foo"));
		assertEquals("\"select\"", CassandraCqlUtils.validTableName("select"));
	}
	
	@Test
	public void testValidColumnName() {
		assertEquals("foo", CassandraCqlUtils.validColumnName("foo"));
		assertEquals("\"\"", CassandraCqlUtils.validColumnName(CassandraCqlUtils.EMPTY_COLUMN_NAME));
		assertEquals("\"\"", CassandraCqlUtils.validColumnName(""));
		assertEquals("\"select\"", CassandraCqlUtils.validColumnName("select"));
	}

	@Test
	public void testQuote() {
		assertEquals("'foo'", CassandraCqlUtils.quote("foo"));
		assertEquals("'Presto''s'", CassandraCqlUtils.quote("Presto's"));
	}

	@Test
	public void testAppendSelectColumns() {
		StringBuilder sb = new StringBuilder();
		CassandraColumnHandle col0 = new CassandraColumnHandle("", "foo", 0, CassandraType.VARCHAR,
				null, false, false);
		CassandraColumnHandle col1 = new CassandraColumnHandle("", "bar", 0, CassandraType.VARCHAR,
				null, false, false);
		CassandraColumnHandle col2 = new CassandraColumnHandle("", "table", 0,
				CassandraType.VARCHAR, null, false, false);
		List<? extends ColumnHandle> columns = Arrays.asList(col0, col1, col2);
		CassandraCqlUtils.appendSelectColumns(sb, columns);
		String str = sb.toString();
		assertEquals("foo,bar,\"table\"", str);
	}

	@Test
	public void testToHostAddressList() throws Exception {
		CassandraCqlUtils utils = new CassandraCqlUtils();
		Set<Host> hosts = new LinkedHashSet<>();
		hosts.add(new TestHost(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
				11, 12, 13, 14, 15, 16 })));
		hosts.add(new TestHost(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 })));
		final List<HostAddress> list = utils.toHostAddressList(hosts);

		assertEquals("[[102:304:506:708:90a:b0c:d0e:f10], 1.2.3.4]", list.toString());
	}
}
