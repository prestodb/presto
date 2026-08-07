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

import com.facebook.presto.iceberg.delete.DeleteFile;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergAbstractMetadata.replaceDeletionVectors;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.toIcebergDeletionVector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestIcebergDeletionVectorCommit
{
    private static final String DATA_FILE = "/wh/db/t/data/file.parquet";
    private static final String DV_PATH = "/wh/db/t/deletes/dv.puffin";

    private static DeleteFile prestoDeletionVector(Optional<Long> contentOffset, Optional<Long> contentSizeInBytes)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                DV_PATH,
                FileFormat.PUFFIN,
                10L,
                200L,
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                contentOffset,
                contentSizeInBytes,
                Optional.of(DATA_FILE),
                3L);
    }

    @Test
    public void testToIcebergDeletionVectorHappyPath()
    {
        org.apache.iceberg.DeleteFile dv = toIcebergDeletionVector(
                DATA_FILE,
                prestoDeletionVector(Optional.of(42L), Optional.of(128L)));

        assertEquals(dv.path().toString(), DV_PATH);
        assertEquals(dv.format(), org.apache.iceberg.FileFormat.PUFFIN);
        assertEquals(dv.referencedDataFile(), DATA_FILE);
        assertEquals(dv.contentOffset(), Long.valueOf(42L));
        assertEquals(dv.contentSizeInBytes(), Long.valueOf(128L));
        assertEquals(dv.recordCount(), 10L);
        assertEquals(dv.fileSizeInBytes(), 200L);
    }

    @Test
    public void testToIcebergDeletionVectorMissingContentOffset()
    {
        VerifyException e = expectThrows(VerifyException.class, () ->
                toIcebergDeletionVector(DATA_FILE, prestoDeletionVector(Optional.empty(), Optional.of(128L))));
        assertTrue(e.getMessage().contains("contentOffset"));
    }

    @Test
    public void testToIcebergDeletionVectorMissingContentSizeInBytes()
    {
        VerifyException e = expectThrows(VerifyException.class, () ->
                toIcebergDeletionVector(DATA_FILE, prestoDeletionVector(Optional.of(42L), Optional.empty())));
        assertTrue(e.getMessage().contains("contentSizeInBytes"));
    }

    @Test
    public void testReplaceDeletionVectorsEmptyMapIsNoOp()
    {
        // An empty map returns before touching the RowDelta, so a null RowDelta is safe here.
        replaceDeletionVectors(null, Optional.empty(), ImmutableMap.of(), dataFile -> true);
    }

    @Test
    public void testReplaceDeletionVectorsRequiresReadSnapshot()
    {
        // A non-empty DV map with no read snapshot is a lost-update hazard and must fail loudly
        // before any DV is removed (so the null RowDelta is never dereferenced).
        VerifyException e = expectThrows(VerifyException.class, () ->
                replaceDeletionVectors(
                        null,
                        Optional.empty(),
                        ImmutableMap.of(DATA_FILE, prestoDeletionVector(Optional.of(42L), Optional.of(128L))),
                        dataFile -> true));
        assertTrue(e.getMessage().contains("readSnapshotId"));
    }
}
