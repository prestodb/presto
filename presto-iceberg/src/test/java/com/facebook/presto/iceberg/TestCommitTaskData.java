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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import org.apache.iceberg.Metrics;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestCommitTaskData
{
    private static final JsonCodec<CommitTaskData> CODEC = jsonCodec(CommitTaskData.class);

    private static MetricsWrapper metrics(long recordCount)
    {
        return new MetricsWrapper(new Metrics(recordCount, null, null, null, null, null, null));
    }

    @Test
    public void testRoundTripWithDeletionVectorFields()
    {
        // A V3 deletion-vector commit carries the Puffin blob offset/size alongside
        // the referenced data file.
        CommitTaskData expected = new CommitTaskData(
                "/warehouse/db/t/deletes/dv.puffin",
                1024L,
                metrics(5L),
                0,
                Optional.empty(),
                FileFormat.PUFFIN,
                "/warehouse/db/t/data/file.parquet",
                FileContent.DELETION_VECTOR,
                Optional.of(42L),
                Optional.of(128L));

        CommitTaskData actual = CODEC.fromJson(CODEC.toJson(expected));

        assertEquals(actual.getContentOffset(), Optional.of(42L));
        assertEquals(actual.getContentSizeInBytes(), Optional.of(128L));
        assertEquals(actual.getReferencedDataFile(), Optional.of("/warehouse/db/t/data/file.parquet"));
        assertEquals(actual.getContent(), FileContent.DELETION_VECTOR);
    }

    @Test
    public void testPartitionSpecIdSurvivesRoundTrip()
    {
        // The wire key is "partitionSpecJson"; both the @JsonCreator parameter and the getter must
        // use it or a Java-produced fragment silently decodes as spec id 0 and the commit picks
        // the wrong partition spec after partition evolution. Velox writes the same key
        // (IcebergDataSink.cpp, IcebergDeletionVectorSink.cpp), so this also pins Java/native
        // agreement. A non-zero id is required: spec id 0 cannot distinguish a dropped field from
        // a decoded one.
        CommitTaskData expected = new CommitTaskData(
                "/warehouse/db/t/deletes/pos.parquet",
                2048L,
                metrics(3L),
                7,
                Optional.of("[\"A\"]"),
                FileFormat.PARQUET,
                "/warehouse/db/t/data/file.parquet",
                FileContent.POSITION_DELETES,
                Optional.empty(),
                Optional.empty());

        String json = CODEC.toJson(expected);
        assertTrue(json.replaceAll("\\s", "").contains("\"partitionSpecJson\":7"),
                "serialized fragment must use the partitionSpecJson wire key: " + json);
        assertEquals(CODEC.fromJson(json).getPartitionSpecId(), 7);
    }

    @Test
    public void testNativeFragmentDecodesPartitionSpecId()
    {
        // Byte-for-byte shape of what IcebergDeletionVectorSink.cpp emits.
        String nativeJson = "{" +
                "\"path\":\"/warehouse/db/t/deletes/dv.puffin\"," +
                "\"fileSizeInBytes\":1024," +
                "\"metrics\":{\"recordCount\":5}," +
                "\"partitionSpecJson\":7," +
                "\"fileFormat\":\"PUFFIN\"," +
                "\"referencedDataFile\":\"/warehouse/db/t/data/file.parquet\"," +
                "\"content\":\"POSITION_DELETES\"," +
                "\"contentOffset\":42," +
                "\"contentSizeInBytes\":128}";

        CommitTaskData actual = CODEC.fromJson(nativeJson);

        assertEquals(actual.getPartitionSpecId(), 7);
        assertEquals(actual.getContentOffset(), Optional.of(42L));
        assertEquals(actual.getContentSizeInBytes(), Optional.of(128L));
    }

    @Test
    public void testBackwardCompatibilityMissingDeletionVectorFields()
    {
        // A V2 position-delete fragment produced by an older binary omits the V3
        // contentOffset/contentSizeInBytes keys entirely; deserialization must
        // tolerate their absence and default to Optional.empty().
        String legacyJson = "{" +
                "\"path\":\"/warehouse/db/t/deletes/pos.parquet\"," +
                "\"fileSizeInBytes\":2048," +
                "\"metrics\":" + jsonCodec(MetricsWrapper.class).toJson(metrics(3L)) + "," +
                "\"partitionSpecJson\":0," +
                "\"fileFormat\":\"PARQUET\"," +
                "\"referencedDataFile\":\"/warehouse/db/t/data/file.parquet\"," +
                "\"content\":\"POSITION_DELETES\"}";

        CommitTaskData actual = CODEC.fromJson(legacyJson);

        assertFalse(actual.getContentOffset().isPresent());
        assertFalse(actual.getContentSizeInBytes().isPresent());
        assertEquals(actual.getContent(), FileContent.POSITION_DELETES);
    }
}
