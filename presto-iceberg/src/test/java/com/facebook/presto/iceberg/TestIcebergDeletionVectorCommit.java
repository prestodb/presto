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
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.iceberg.IcebergAbstractMetadata.buildDeletionVectorEntry;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.recordAddedDeletionVector;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.replaceDeletionVectors;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.toIcebergDeletionVector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestIcebergDeletionVectorCommit
{
    private static final String DATA_FILE = "/wh/db/t/data/file.parquet";
    private static final String DV_PATH = "/wh/db/t/deletes/dv.puffin";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "part", Types.StringType.get()));
    private static final PartitionSpec PARTITIONED_SPEC = PartitionSpec.builderFor(SCHEMA).identity("part").build();

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
                prestoDeletionVector(Optional.of(42L), Optional.of(128L)),
                PartitionSpec.unpartitioned(),
                null);

        assertEquals(dv.path().toString(), DV_PATH);
        assertEquals(dv.format(), org.apache.iceberg.FileFormat.PUFFIN);
        assertEquals(dv.referencedDataFile(), DATA_FILE);
        assertEquals(dv.contentOffset(), Long.valueOf(42L));
        assertEquals(dv.contentSizeInBytes(), Long.valueOf(128L));
        assertEquals(dv.recordCount(), 10L);
        assertEquals(dv.fileSizeInBytes(), 200L);
    }

    /**
     * A reconstruction for a partitioned table must carry the partition. Iceberg prunes candidate
     * manifests by partition before matching a delete for removal, so an unpartitioned
     * reconstruction never reaches the manifest holding the prior DV and silently leaves it
     * behind, giving the data file two deletion vectors.
     */
    @Test
    public void testToIcebergDeletionVectorCarriesPartition()
    {
        PartitionData partition = new PartitionData(PARTITIONED_SPEC.partitionType());
        partition.set(0, "A");

        org.apache.iceberg.DeleteFile dv = toIcebergDeletionVector(
                DATA_FILE,
                prestoDeletionVector(Optional.of(42L), Optional.of(128L)),
                PARTITIONED_SPEC,
                partition);

        assertEquals(dv.specId(), PARTITIONED_SPEC.specId());
        assertEquals(dv.partition().get(0, String.class), "A");
    }

    @Test
    public void testToIcebergDeletionVectorMissingContentOffset()
    {
        VerifyException e = expectThrows(VerifyException.class, () ->
                toIcebergDeletionVector(DATA_FILE, prestoDeletionVector(Optional.empty(), Optional.of(128L)), PartitionSpec.unpartitioned(), null));
        assertTrue(e.getMessage().contains("contentOffset"));
    }

    @Test
    public void testToIcebergDeletionVectorMissingContentSizeInBytes()
    {
        VerifyException e = expectThrows(VerifyException.class, () ->
                toIcebergDeletionVector(DATA_FILE, prestoDeletionVector(Optional.of(42L), Optional.empty()), PartitionSpec.unpartitioned(), null));
        assertTrue(e.getMessage().contains("contentSizeInBytes"));
    }

    @Test
    public void testReplaceDeletionVectorsEmptyMapIsNoOp()
    {
        // An empty map must return before touching the RowDelta. Prove the no-op by passing a
        // proxy RowDelta that records any method invocation, then asserting it was never touched
        // (a null RowDelta would only prove no NPE, not that no mutation was attempted).
        AtomicBoolean touched = new AtomicBoolean(false);
        RowDelta rowDelta = (RowDelta) Proxy.newProxyInstance(
                RowDelta.class.getClassLoader(),
                new Class<?>[] {RowDelta.class},
                (proxy, method, args) -> {
                    touched.set(true);
                    return null;
                });

        replaceDeletionVectors(rowDelta, Optional.empty(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

        assertFalse(touched.get(), "empty deletion-vector map must not touch the RowDelta");
    }

    @Test
    public void testReplaceDeletionVectorsRequiresReadSnapshot()
    {
        // A non-empty DV map with no read snapshot means the read snapshot expired before commit
        // and the RowDelta was never bound, which is a lost-update hazard. It must fail loudly
        // before any DV is removed (so the null RowDelta is never dereferenced).
        // The added-DV map is non-empty so the removal loop would run (and dereference the null
        // RowDelta) if the readSnapshotId check did not fire first.
        org.apache.iceberg.DeleteFile added = toIcebergDeletionVector(
                DATA_FILE,
                prestoDeletionVector(Optional.of(42L), Optional.of(128L)),
                PartitionSpec.unpartitioned(),
                null);
        VerifyException e = expectThrows(VerifyException.class, () ->
                replaceDeletionVectors(
                        null,
                        Optional.empty(),
                        ImmutableMap.of(DATA_FILE, prestoDeletionVector(Optional.of(42L), Optional.of(128L))),
                        ImmutableMap.of(DATA_FILE, added),
                        ImmutableMap.of(added.specId(), PartitionSpec.unpartitioned())));
        assertTrue(e.getMessage().contains("must be bound to the read snapshot"));
    }

    private static CommitTaskData deletionVectorFragment(Optional<Long> contentOffset, Optional<Long> contentSizeInBytes)
    {
        return new CommitTaskData(
                DV_PATH,
                200L,
                new MetricsWrapper(new Metrics(10L, null, null, null, null, null, null)),
                PartitionSpec.unpartitioned().specId(),
                Optional.empty(),
                com.facebook.presto.iceberg.FileFormat.PUFFIN,
                DATA_FILE,
                FileContent.POSITION_DELETES,
                contentOffset,
                contentSizeInBytes);
    }

    /**
     * A V3 POSITION_DELETES fragment with no Puffin coordinates is what the Java page sink
     * produces. It must surface the actionable native-only error at unit level, not only when an
     * end-to-end query happens to exercise it.
     */
    @Test
    public void testDeletionVectorFragmentWithoutCoordinatesIsNativeOnly()
    {
        PrestoException e = expectThrows(PrestoException.class, () ->
                buildDeletionVectorEntry(
                        deletionVectorFragment(Optional.empty(), Optional.empty()),
                        PartitionSpec.unpartitioned(),
                        new Type[0]));
        assertTrue(e.getMessage().contains("require the native (Prestissimo) worker"), e.getMessage());
    }

    @Test(dataProvider = "halfSetCoordinates")
    public void testDeletionVectorFragmentWithOneCoordinateIsRejected(Optional<Long> contentOffset, Optional<Long> contentSizeInBytes)
    {
        // Exactly one coordinate set is a malformed fragment. It must fail with a message naming
        // both fields rather than falling through to the V2 Parquet builder (which would emit a
        // corrupt position-delete file for a V3 table).
        VerifyException e = expectThrows(VerifyException.class, () ->
                buildDeletionVectorEntry(
                        deletionVectorFragment(contentOffset, contentSizeInBytes),
                        PartitionSpec.unpartitioned(),
                        new Type[0]));
        assertTrue(e.getMessage().contains("contentOffset") && e.getMessage().contains("contentSizeInBytes"), e.getMessage());
    }

    @DataProvider(name = "halfSetCoordinates")
    public Object[][] halfSetCoordinates()
    {
        return new Object[][] {
                {Optional.of(42L), Optional.empty()},
                {Optional.empty(), Optional.of(128L)},
        };
    }

    /**
     * Iceberg V3 allows one deletion vector per data file. Two fragments in the same commit for
     * one data file would silently leave that file with two DVs, so the commit must fail.
     */
    @Test
    public void testTwoDeletionVectorsForOneDataFileFailTheCommit()
    {
        Map<String, org.apache.iceberg.DeleteFile> addedDeletionVectors = new HashMap<>();
        org.apache.iceberg.DeleteFile first = buildDeletionVectorEntry(
                deletionVectorFragment(Optional.of(0L), Optional.of(128L)), PartitionSpec.unpartitioned(), new Type[0]);
        recordAddedDeletionVector(addedDeletionVectors, DATA_FILE, first);

        PrestoException e = expectThrows(PrestoException.class, () ->
                recordAddedDeletionVector(addedDeletionVectors, DATA_FILE, first));
        assertTrue(e.getMessage().contains("one deletion vector per data file"), e.getMessage());
        assertTrue(e.getMessage().contains(DATA_FILE), e.getMessage());
    }
}
