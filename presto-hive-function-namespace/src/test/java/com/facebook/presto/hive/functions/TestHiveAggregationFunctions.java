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

import com.facebook.airlift.testing.Closeables;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.TestingPrestoClient;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.inject.Key;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

public class TestHiveAggregationFunctions
{
    private static final Type BIGINT_ARRAY = new ArrayType(BIGINT);

    private TestingPrestoServer server;
    private TestingPrestoClient client;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = createServer();
        client = new TestingPrestoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas"))
                .build());
    }

    @AfterClass
    public void destroy()
    {
        Closeables.closeQuietly(server, client);
    }

    @Test
    public void aggregationFunctions()
    {
        check("select hive.default.avg(nationkey) from tpch.tiny.nation",
                column(DOUBLE, 12.0));
        check("select regionkey, hive.default.avg(nationkey) from tpch.tiny.nation group by regionkey order by 1",
                column(BIGINT, 0L, 1L, 2L, 3L, 4L),
                column(DOUBLE, 10.0, 9.4, 13.6, 15.4, 11.6));
        check("select hive.default.collect_list(regionkey) from tpch.tiny.nation",
                column(BIGINT_ARRAY, Arrays.asList(0L, 1L, 1L, 1L, 4L, 0L, 3L, 3L, 2L, 2L, 4L, 4L, 2L, 4L, 0L, 0L, 0L, 1L, 2L, 3L, 4L, 2L, 3L, 3L, 1L)));
        check("select hive.default.collect_set(regionkey) from tpch.tiny.nation",
                column(BIGINT_ARRAY, Arrays.asList(0L, 1L, 4L, 3L, 2L)));
        check("select hive.default.corr(nationkey, regionkey) from tpch.tiny.nation",
                column(DOUBLE, 0.18042685));
        check("select hive.default.covar_pop(nationkey, regionkey) from tpch.tiny.nation",
                column(DOUBLE, 1.84));
        check("select hive.default.covar_samp(nationkey, regionkey) from tpch.tiny.nation",
                column(DOUBLE, 1.9166666));
        check("select hive.default.max(name) from tpch.tiny.nation",
                column(VARCHAR, "VIETNAM"));
        check("select regionkey, hive.default.max(name) from tpch.tiny.nation group by regionkey order by 1",
                column(BIGINT, 0L, 1L, 2L, 3L, 4L),
                column(VARCHAR, "MOZAMBIQUE", "UNITED STATES", "VIETNAM", "UNITED KINGDOM", "SAUDI ARABIA"));
        check("select hive.default.min(name) from tpch.tiny.nation",
                column(VARCHAR, "ALGERIA"));
        check("select regionkey, hive.default.min(name) from tpch.tiny.nation group by regionkey order by 1",
                column(BIGINT, 0L, 1L, 2L, 3L, 4L),
                column(VARCHAR, "ALGERIA", "ARGENTINA", "CHINA", "FRANCE", "EGYPT"));
        check("select hive.default.std(nationkey) from tpch.tiny.nation",
                column(DOUBLE, 7.2111025));
        check("select regionkey, hive.default.std(nationkey) from tpch.tiny.nation group by regionkey order by 1",
                column(BIGINT, 0L, 1L, 2L, 3L, 4L),
                column(DOUBLE, 6.35609943, 9.35093578, 5.08330601, 7.39188744, 5.16139516));
        check("select hive.default.sum(nationkey) from tpch.tiny.nation",
                column(BIGINT, 300L));
        check("select regionkey, hive.default.sum(nationkey) from tpch.tiny.nation group by regionkey order by 1",
                column(BIGINT, 0L, 1L, 2L, 3L, 4L),
                column(BIGINT, 50L, 47L, 68L, 77L, 58L));
        check("select hive.default.variance(nationkey) from tpch.tiny.nation",
                column(DOUBLE, 52.0));
        check("select hive.default.var_samp(nationkey) from tpch.tiny.nation",
                column(DOUBLE, 54.1666666));
    }

    @SuppressWarnings("UnknownLanguage")
    public void check(@Language("SQL") String query, Column... expectedColumns)
    {
        checkArgument(expectedColumns != null && expectedColumns.length > 0);
        int numColumns = expectedColumns.length;
        int numRows = expectedColumns[0].values.length;
        checkArgument(Stream.of(expectedColumns).allMatch(c -> c != null && c.values.length == numRows));

        MaterializedResult result = client.execute(query).getResult();
        assertEquals(result.getRowCount(), numRows);

        for (int i = 0; i < numColumns; i++) {
            assertEquals(result.getTypes().get(i), expectedColumns[i].type);
        }
        List<MaterializedRow> rows = result.getMaterializedRows();
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numColumns; j++) {
                Object actual = rows.get(i).getField(j);
                Object expected = expectedColumns[j].values[i];
                if (expectedColumns[j].type == DOUBLE) {
                    assertEquals(((Number) actual).doubleValue(), ((double) expected), 0.000001);
                }
                else {
                    assertEquals(actual, expected);
                }
            }
        }
    }

    private static TestingPrestoServer createServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.installPlugin(new HiveFunctionNamespacePlugin());
        server.createCatalog("tpch", "tpch");
        FunctionAndTypeManager functionAndTypeManager = server.getInstance(Key.get(FunctionAndTypeManager.class));
        functionAndTypeManager.loadFunctionNamespaceManager(
                "hive-functions",
                "hive",
                Collections.emptyMap());
        server.refreshNodes();
        return server;
    }

    public static Column column(Type type, Object... values)
    {
        return new Column(type, values);
    }

    private static class Column
    {
        private final Type type;

        private final Object[] values;

        private Column(Type type, Object[] values)
        {
            this.type = type;
            this.values = values;
        }
    }
}
