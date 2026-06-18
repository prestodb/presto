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

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestIcebergTableName
{
    @Test
    public void testFrom()
    {
        assertFrom("abc", "abc", IcebergTableType.DATA);
        assertFrom("abc@123", "abc", IcebergTableType.DATA, Optional.of(123L));
        assertFrom("abc$data", "abc", IcebergTableType.DATA);
        assertFrom("xyz@456", "xyz", IcebergTableType.DATA, Optional.of(456L));
        assertFrom("xyz$data@456", "xyz", IcebergTableType.DATA, Optional.of(456L));
        assertFrom("abc$partitions@456", "abc", IcebergTableType.PARTITIONS, Optional.of(456L));
        assertFrom("abc$manifests@456", "abc", IcebergTableType.MANIFESTS, Optional.of(456L));
        assertFrom("abc$manifests@456", "abc", IcebergTableType.MANIFESTS, Optional.of(456L));
        assertFrom("abc$history", "abc", IcebergTableType.HISTORY);
        assertFrom("abc$snapshots", "abc", IcebergTableType.SNAPSHOTS);
        assertFrom("abc$changelog", "abc", IcebergTableType.CHANGELOG);
        assertFrom("abc@123$changelog", "abc", IcebergTableType.CHANGELOG, Optional.of(123L));
        assertFrom("abc$changelog@123", "abc", IcebergTableType.CHANGELOG, Optional.empty(), Optional.of(123L));
        assertFrom("abc@123$changelog@124", "abc", IcebergTableType.CHANGELOG, Optional.of(123L), Optional.of(124L));

        assertInvalid("abc@xyz", "Invalid Iceberg table name: abc@xyz");
        assertInvalid("abc$what", "Invalid Iceberg table name (unknown type 'what'): abc$what");
        assertInvalid("abc@123$data@456", "Invalid Iceberg table name (cannot specify two @ versions): abc@123$data@456");
        assertInvalid("abc@123$snapshots", "Invalid Iceberg table name (cannot use @ version with table type 'SNAPSHOTS'): abc@123$snapshots");
        assertInvalid("abc$snapshots@456", "Invalid Iceberg table name (cannot use @ version with table type 'SNAPSHOTS'): abc$snapshots@456");
    }

    private static void assertInvalid(String inputName, String message)
    {
        try {
            IcebergTableName.from(inputName);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), message);
        }
    }

    private static void assertFrom(String inputName, String tableName, IcebergTableType icebergTableType, Optional<Long> snapshotId, Optional<Long> changelogEndSnapshot)
    {
        IcebergTableName name = IcebergTableName.from(inputName);
        assertEquals(name.getTableName(), tableName);
        assertEquals(name.getTableType(), icebergTableType);
        assertEquals(name.getSnapshotId(), snapshotId);
        assertEquals(name.getChangelogEndSnapshot(), changelogEndSnapshot);
    }

    private static void assertFrom(String inputName, String tableName, IcebergTableType icebergTableType, Optional<Long> snapshotId)
    {
        assertFrom(inputName, tableName, icebergTableType, snapshotId, Optional.empty());
    }

    private static void assertFrom(String inputName, String tableName, IcebergTableType icebergTableType)
    {
        assertFrom(inputName, tableName, icebergTableType, Optional.empty(), Optional.empty());
    }

    @Test
    public void testBranchParsing()
    {
        // Basic branch parsing
        assertFromWithBranch("orders.branch_audit_branch", "orders", IcebergTableType.DATA, Optional.empty(), Optional.of("audit_branch"));
        assertFromWithBranch("customers.branch_dev", "customers", IcebergTableType.DATA, Optional.empty(), Optional.of("dev"));
        assertFromWithBranch("table.branch_feature_123", "table", IcebergTableType.DATA, Optional.empty(), Optional.of("feature_123"));
        // Branch with underscores and hyphens
        assertFromWithBranch("orders.branch_audit_branch_v2", "orders", IcebergTableType.DATA, Optional.empty(), Optional.of("audit_branch_v2"));
        assertFromWithBranch("orders.branch_test-branch", "orders", IcebergTableType.DATA, Optional.empty(), Optional.of("test-branch"));
        // Branch with table types (allowed combinations)
        assertFromWithBranch("orders.branch_audit$history", "orders", IcebergTableType.HISTORY, Optional.empty(), Optional.of("audit"));
        assertFromWithBranch("orders.branch_audit$snapshots", "orders", IcebergTableType.SNAPSHOTS, Optional.empty(), Optional.of("audit"));
        assertFromWithBranch("orders.branch_audit$partitions", "orders", IcebergTableType.PARTITIONS, Optional.empty(), Optional.of("audit"));
        assertFromWithBranch("orders.branch_audit$manifests", "orders", IcebergTableType.MANIFESTS, Optional.empty(), Optional.of("audit"));
        assertFromWithBranch("orders.branch_audit$files", "orders", IcebergTableType.FILES, Optional.empty(), Optional.of("audit"));
        assertFromWithBranch("orders.branch_audit$changelog", "orders", IcebergTableType.CHANGELOG, Optional.empty(), Optional.of("audit"));
        // Branch with snapshot version should be rejected (branches and snapshots are mutually exclusive)
        assertInvalid("orders.branch_audit@123", "Invalid Iceberg table name (cannot use @ version with branch): orders.branch_audit@123");
        assertInvalid("orders.branch_audit$data@123", "Invalid Iceberg table name (cannot use @ version with branch): orders.branch_audit$data@123");
        assertInvalid("orders.branch_audit$partitions@456", "Invalid Iceberg table name (cannot use @ version with branch): orders.branch_audit$partitions@456");
        // Verify no branch is parsed for regular tables
        assertFromWithBranch("orders", "orders", IcebergTableType.DATA, Optional.empty(), Optional.empty());
        assertFromWithBranch("orders@123", "orders", IcebergTableType.DATA, Optional.of(123L), Optional.empty());
        assertFromWithBranch("orders$history", "orders", IcebergTableType.HISTORY, Optional.empty(), Optional.empty());
    }

    @Test
    public void testBranchJsonRoundTrip()
    {
        // Test JSON serialization/deserialization preserves branchName
        IcebergTableName original = IcebergTableName.from("orders.branch_audit_branch");
        assertEquals(original.getTableName(), "orders");
        assertEquals(original.getTableType(), IcebergTableType.DATA);
        assertEquals(original.getBranchName(), Optional.of("audit_branch"));
        assertEquals(original.getSnapshotId(), Optional.empty());
        // Create a new instance with same values (simulating JSON round-trip)
        IcebergTableName roundTrip = new IcebergTableName(
                original.getTableName(),
                original.getTableType(),
                original.getSnapshotId(),
                original.getBranchName(),
                original.getChangelogEndSnapshot());
        assertEquals(roundTrip.getTableName(), "orders");
        assertEquals(roundTrip.getTableType(), IcebergTableType.DATA);
        assertEquals(roundTrip.getBranchName(), Optional.of("audit_branch"));
        assertEquals(roundTrip.getSnapshotId(), Optional.empty());
        // Test with branch and table type
        IcebergTableName withType = IcebergTableName.from("orders.branch_audit$history");
        assertEquals(withType.getTableName(), "orders");
        assertEquals(withType.getTableType(), IcebergTableType.HISTORY);
        assertEquals(withType.getBranchName(), Optional.of("audit"));
        IcebergTableName withTypeRoundTrip = new IcebergTableName(
                withType.getTableName(),
                withType.getTableType(),
                withType.getSnapshotId(),
                withType.getBranchName(),
                withType.getChangelogEndSnapshot());
        assertEquals(withTypeRoundTrip.getBranchName(), Optional.of("audit"));
    }

    private static void assertFromWithBranch(String inputName, String tableName, IcebergTableType icebergTableType, Optional<Long> snapshotId, Optional<String> branchName)
    {
        IcebergTableName name = IcebergTableName.from(inputName);
        assertEquals(name.getTableName(), tableName);
        assertEquals(name.getTableType(), icebergTableType);
        assertEquals(name.getSnapshotId(), snapshotId);
        assertEquals(name.getBranchName(), branchName);
    }
}
