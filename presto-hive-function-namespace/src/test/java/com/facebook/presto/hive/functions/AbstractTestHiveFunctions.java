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

package com.facebook.presto.hive.functions;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import com.google.inject.Key;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public abstract class AbstractTestHiveFunctions
{
    private static final Logger log = Logger.get(AbstractTestHiveFunctions.class);

    protected TestingPrestoServer server;
    protected TestingPrestoClient client;
    protected TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        // TODO: Use DistributedQueryRunner to perform query
        server = createTestingPrestoServer();
        client = new TestingPrestoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas"))
                .build());
        typeManager = server.getInstance(Key.get(TypeManager.class));

        if (getInitScript().isPresent()) {
            String sql = Files.asCharSource(getInitScript().get(), UTF_8).read();
            Iterable<String> initQueries = Splitter.on("----\n").omitEmptyStrings().trimResults().split(sql);
            for (@Language("SQL") String query : initQueries) {
                log.debug("Executing %s", query);
                client.execute(query);
            }
        }
    }

    public void assertFunction(String expr, Type type, Object value)
    {
        assertQuery("SELECT " + expr, Column.of(type, value));
    }

    public void assertInvalidFunction(String expr, String exceptionPattern)
    {
        try {
            client.execute("SELECT " + expr);
            fail("Function expected to fail but not");
        }
        catch (Exception e) {
            if (!(e.getMessage().matches(exceptionPattern))) {
                fail(format("Expected exception message '%s' to match '%s' but not",
                        e.getMessage(), exceptionPattern));
            }
        }
    }

    public void assertQuery(@Language("SQL") String sql, Column... cols)
    {
        checkArgument(cols != null && cols.length > 0);
        int numColumns = cols.length;
        int numRows = cols[0].values.length;
        checkArgument(Stream.of(cols).allMatch(c -> c != null && c.values.length == numRows));

        MaterializedResult result = client.execute(sql).getResult();
        assertEquals(result.getRowCount(), numRows);

        for (int i = 0; i < numColumns; i++) {
            assertEquals(result.getTypes().get(i), cols[i].type);
        }
        List<MaterializedRow> rows = result.getMaterializedRows();
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numColumns; j++) {
                Object actual = rows.get(i).getField(j);
                Object expected = cols[j].values[i];
                if (cols[j].type == DOUBLE) {
                    assertEquals(((Number) actual).doubleValue(), ((double) expected), 0.000001);
                }
                else {
                    assertEquals(actual, expected);
                }
            }
        }
    }

    protected Optional<File> getInitScript()
    {
        return Optional.empty();
    }

    public static class Column
    {
        private final Type type;

        private final Object[] values;

        public static Column of(Type type, Object... values)
        {
            return new Column(type, values);
        }

        private Column(Type type, Object[] values)
        {
            this.type = type;
            this.values = values;
        }
    }
}
