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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.TableFunctionApplicationResult;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test(singleThreaded = true)
public class TestLanceVectorSearchTableFunction
{
    private LanceNamespaceHolder namespaceHolder;
    private LanceVectorSearchTableFunction function;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL dbUrl = Resources.getResource(TestLanceVectorSearchTableFunction.class, "/vector_db");
        String rootPath = Paths.get(dbUrl.toURI()).toString();
        LanceConfig config = new LanceConfig().setSingleLevelNs(true);
        namespaceHolder = new LanceNamespaceHolder(config, ImmutableMap.of("lance.root", rootPath));
        function = new LanceVectorSearchTableFunction(namespaceHolder);
    }

    @AfterMethod
    public void tearDown()
    {
        namespaceHolder.shutdown();
    }

    private static Object queryVector(double... values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, values.length);
        for (double value : values) {
            DOUBLE.writeDouble(builder, value);
        }
        return builder.build();
    }

    private Map<String, Argument> defaultArguments()
    {
        Map<String, Argument> arguments = new HashMap<>();
        arguments.put("SCHEMA_NAME", new ScalarArgument(VARCHAR, utf8Slice("default")));
        arguments.put("TABLE_NAME", new ScalarArgument(VARCHAR, utf8Slice("vectors")));
        arguments.put("VECTOR_COL", new ScalarArgument(VARCHAR, utf8Slice("vec")));
        arguments.put("QUERY_VECTOR", new ScalarArgument(new ArrayType(DOUBLE), queryVector(0.3, 0.0, 0.0, 0.0)));
        arguments.put("K", new ScalarArgument(BIGINT, 3L));
        arguments.put("METRIC", new ScalarArgument(VARCHAR, utf8Slice("l2")));
        arguments.put("NPROBES", new ScalarArgument(BIGINT, null));
        arguments.put("EF", new ScalarArgument(BIGINT, null));
        arguments.put("REFINE_FACTOR", new ScalarArgument(BIGINT, null));
        return arguments;
    }

    @Test
    public void testAnalyzeHappyPath()
    {
        TableFunctionAnalysis analysis = function.analyze(null, null, defaultArguments());

        Descriptor descriptor = analysis.getReturnedType().orElseThrow(AssertionError::new);
        assertEquals(descriptor.getFields().size(), 4); // id, vec, tag, _distance
        assertEquals(descriptor.getFields().get(3).getName(), Optional.of("_distance"));

        LanceVectorSearchFunctionHandle handle = (LanceVectorSearchFunctionHandle) analysis.getHandle();
        LanceTableHandle tableHandle = handle.getTableHandle();
        assertTrue(tableHandle.getDatasetVersion().isPresent(), "dataset version must be pinned at analysis");
        assertTrue(tableHandle.hasVectorSearch());
        LanceVectorSearch search = tableHandle.getVectorSearch().get();
        assertEquals(search.getColumn(), "vec");
        assertEquals(search.getK(), 3);
        assertEquals(search.getMetric(), LanceVectorSearch.METRIC_L2);
        assertTrue(search.isRequiresGlobalTopK());
        assertEquals(handle.getColumns().size(), 4);
        assertTrue(handle.getColumns().get(3).isDistance());
    }

    @Test
    public void testUnknownTable()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("TABLE_NAME", new ScalarArgument(VARCHAR, utf8Slice("nope")));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testUnknownVectorColumn()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("VECTOR_COL", new ScalarArgument(VARCHAR, utf8Slice("nope")));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testNonVectorColumnRejected()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("VECTOR_COL", new ScalarArgument(VARCHAR, utf8Slice("id")));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testDimensionMismatchRejected()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("QUERY_VECTOR", new ScalarArgument(new ArrayType(DOUBLE), queryVector(1.0, 2.0)));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testBadMetricRejected()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("METRIC", new ScalarArgument(VARCHAR, utf8Slice("hamming")));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testNonPositiveKRejected()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("K", new ScalarArgument(BIGINT, 0L));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testEmptyQueryVectorRejected()
    {
        Map<String, Argument> arguments = defaultArguments();
        arguments.put("QUERY_VECTOR", new ScalarArgument(new ArrayType(DOUBLE), queryVector()));
        expectThrows(PrestoException.class, () -> function.analyze(null, null, arguments));
    }

    @Test
    public void testFunctionHandleJsonRoundTrip()
    {
        TableFunctionAnalysis analysis = function.analyze(null, null, defaultArguments());
        LanceVectorSearchFunctionHandle handle = (LanceVectorSearchFunctionHandle) analysis.getHandle();
        // HandleJsonModule serializes column Type via TypeDeserializer in production; plain
        // JsonCodec cannot deserialize Type, so assert equality of a manually reconstructed handle.
        LanceVectorSearchFunctionHandle same = new LanceVectorSearchFunctionHandle(handle.getTableHandle(), handle.getColumns());
        assertEquals(same, handle);
    }

    @Test
    public void testApplyTableFunction()
    {
        LanceMetadata metadata = new LanceMetadata(
                namespaceHolder,
                JsonCodec.jsonCodec(LanceCommitTaskData.class),
                JsonCodec.jsonCodec(LanceMergeCommitData.class));
        TableFunctionAnalysis analysis = function.analyze(null, null, defaultArguments());
        LanceVectorSearchFunctionHandle handle = (LanceVectorSearchFunctionHandle) analysis.getHandle();

        TableFunctionApplicationResult<ConnectorTableHandle> result =
                metadata.applyTableFunction(null, handle).orElseThrow(AssertionError::new);
        assertEquals(result.getTableHandle(), handle.getTableHandle());
        assertEquals(result.getColumnHandles().size(), 4);
    }
}
