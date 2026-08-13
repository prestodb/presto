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
    public void testBackwardCompatibilityMissingDeletionVectorFields()
    {
        // A V2 position-delete fragment produced by an older binary omits the V3
        // contentOffset/contentSizeInBytes keys entirely; deserialization must
        // tolerate their absence and default to Optional.empty().
        String legacyJson = "{" +
                "\"path\":\"/warehouse/db/t/deletes/pos.parquet\"," +
                "\"fileSizeInBytes\":2048," +
                "\"metrics\":" + jsonCodec(MetricsWrapper.class).toJson(metrics(3L)) + "," +
                "\"partitionSpecId\":0," +
                "\"fileFormat\":\"PARQUET\"," +
                "\"referencedDataFile\":\"/warehouse/db/t/data/file.parquet\"," +
                "\"content\":\"POSITION_DELETES\"}";

        CommitTaskData actual = CODEC.fromJson(legacyJson);

        assertFalse(actual.getContentOffset().isPresent());
        assertFalse(actual.getContentSizeInBytes().isPresent());
        assertEquals(actual.getContent(), FileContent.POSITION_DELETES);
    }
}
