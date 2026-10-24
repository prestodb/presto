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
import com.facebook.presto.spi.SchemaTableName;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.IcebergCommitFailures.toPrestoException;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_COMMIT_CONFLICT;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergCommitFailures
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("test_schema", "test_table");

    @Test
    public void testConcurrentModificationIsRetriable()
    {
        ValidationException conflict = new ValidationException("Found new conflicting delete files that can apply to records matching true: [a.parquet]");
        PrestoException failure = toPrestoException(conflict, TABLE_NAME, () -> true);

        assertEquals(failure.getErrorCode(), ICEBERG_COMMIT_CONFLICT.toErrorCode());
        assertTrue(failure.getErrorCode().isRetriable());
        assertEquals(failure.getCause(), conflict);

        String expectedMessage = String.format(
                "Failed to commit changes to the Iceberg table %s because it was concurrently modified",
                TABLE_NAME);
        assertEquals(failure.getMessage(), expectedMessage);
    }

    @Test
    public void testExhaustedIcebergCommitRetriesIsRetriable()
    {
        CommitFailedException commitFailedException = new CommitFailedException("Metadata location changed");
        PrestoException failure = toPrestoException(commitFailedException, TABLE_NAME, () -> true);

        assertEquals(failure.getErrorCode(), ICEBERG_COMMIT_CONFLICT.toErrorCode());
        assertTrue(failure.getErrorCode().isRetriable());
        assertEquals(failure.getCause(), commitFailedException);
    }

    @Test
    public void testValidationFailureWithoutConcurrentChangeIsNotRetriable()
    {
        // e.g. HiveTableOperations reporting an invalid metastore object, which fails again on every attempt
        PrestoException failure = toPrestoException(new ValidationException("Invalid Hive object for test_schema.test_table"), TABLE_NAME, () -> false);

        assertEquals(failure.getErrorCode(), ICEBERG_COMMIT_ERROR.toErrorCode());
        assertFalse(failure.getErrorCode().isRetriable());
    }

    @Test
    public void testUnknownCommitStateIsNeverRetriable()
    {
        // the commit may have been applied, so running the statement again could apply it twice
        PrestoException failure = toPrestoException(
                new CommitStateUnknownException(new RuntimeException("connection reset")),
                TABLE_NAME,
                () -> {
                    throw new AssertionError("the table state must not be consulted for an unknown commit state");
                });

        assertEquals(failure.getErrorCode(), ICEBERG_COMMIT_ERROR.toErrorCode());
        assertFalse(failure.getErrorCode().isRetriable());
    }

    @Test
    public void testUnrelatedFailureIsNotRetriable()
    {
        PrestoException failure = toPrestoException(new IllegalStateException("boom"), TABLE_NAME, () -> true);

        assertEquals(failure.getErrorCode(), ICEBERG_COMMIT_ERROR.toErrorCode());
        assertFalse(failure.getErrorCode().isRetriable());
    }
}
