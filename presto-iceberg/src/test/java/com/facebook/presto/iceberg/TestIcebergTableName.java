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
        assertFrom("abc", "abc", TableType.DATA, Optional.empty());
        assertFrom("abc@123", "abc", TableType.DATA, Optional.of(123L));
        assertFrom("abc$data", "abc", TableType.DATA, Optional.empty());
        assertFrom("xyz@456", "xyz", TableType.DATA, Optional.of(456L));
        assertFrom("xyz$data@456", "xyz", TableType.DATA, Optional.of(456L));
        assertFrom("abc$partitions@456", "abc", TableType.PARTITIONS, Optional.of(456L));
        assertFrom("abc$manifests@456", "abc", TableType.MANIFESTS, Optional.of(456L));
        assertFrom("abc$manifests@456", "abc", TableType.MANIFESTS, Optional.of(456L));
        assertFrom("abc$history", "abc", TableType.HISTORY, Optional.empty());
        assertFrom("abc$snapshots", "abc", TableType.SNAPSHOTS, Optional.empty());

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

    private static void assertFrom(String inputName, String tableName, TableType tableType, Optional<Long> snapshotId)
    {
        IcebergTableName name = IcebergTableName.from(inputName);
        assertEquals(name.getTableName(), tableName);
        assertEquals(name.getTableType(), tableType);
        assertEquals(name.getSnapshotId(), snapshotId);
    }
}
