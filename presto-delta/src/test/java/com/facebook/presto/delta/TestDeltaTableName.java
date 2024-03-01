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
package com.facebook.presto.delta;

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDeltaTableName
{
    private static final DateTimeFormatter TIMESTAMP_PARSER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Test
    public void parsingValidTableNames()
    {
        assertValid("tbl", "tbl");
        assertValid("tbl@v2", "tbl", 2L);
        assertValid("s3://bucket/path/tbl", "s3://bucket/path/tbl");
        assertValid("s3://bucket/path/tbl@v5", "s3://bucket/path/tbl", 5L);
        assertValid("tbl@t2021", "tbl", "2021-01-01 00:00:00");
        assertValid("tbl@t2021-11", "tbl", "2021-11-01 00:00:00");
        assertValid("tbl@t2021-11-18", "tbl", "2021-11-18 00:00:00");
        assertValid("tbl@t2021-11-18 10", "tbl", "2021-11-18 10:00:00");
        assertValid("tbl@t2021-11-18 10:30", "tbl", "2021-11-18 10:30:00");
        assertValid("tbl@t2021-11-18 10:30:03", "tbl", "2021-11-18 10:30:03");
    }

    @Test
    public void parsingInvalidTableNames()
    {
        assertInvalid("sales_table@v-1",
                "Invalid Delta table name: sales_table@v-1, " +
                        "Expected table name form 'tableName[@v<snapshotId>][@t<snapshotAsOfTimestamp>]'.");

        assertInvalid("sales_table@t2021-14",
                "Invalid Delta table name: sales_table@t2021-14, given snapshot timestamp (2021-14) format is not valid");
    }

    private static void assertValid(String inputTableName, String expectedTableName)
    {
        DeltaTableName deltaTableName = DeltaTableName.from(inputTableName);
        assertEquals(deltaTableName.getTableNameOrPath(), expectedTableName);
        assertEquals(deltaTableName.getSnapshotId(), Optional.empty());
        assertEquals(deltaTableName.getTimestampMillisUtc(), Optional.empty());
    }

    private static void assertValid(String inputTableName, String expectedTableName, Long expectedSnapshotId)
    {
        DeltaTableName deltaTableName = DeltaTableName.from(inputTableName);
        assertEquals(deltaTableName.getTableNameOrPath(), expectedTableName);
        assertEquals(deltaTableName.getSnapshotId(), Optional.of(expectedSnapshotId));
        assertEquals(deltaTableName.getTimestampMillisUtc(), Optional.empty());
    }

    private static void assertValid(String inputTableName, String expectedTableName, String expectedTimestamp)
    {
        DeltaTableName deltaTableName = DeltaTableName.from(inputTableName);
        assertEquals(deltaTableName.getTableNameOrPath(), expectedTableName);
        assertEquals(deltaTableName.getSnapshotId(), Optional.empty());
        Long expectedTimestampEpochMillis = LocalDateTime.from(TIMESTAMP_PARSER.parse(expectedTimestamp))
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
        assertEquals(deltaTableName.getTimestampMillisUtc(), Optional.of(expectedTimestampEpochMillis));
    }

    private static void assertInvalid(String inputTableName, String expErrorMessage)
    {
        try {
            DeltaTableName.from(inputTableName);
            fail("expected the above call to fail");
        }
        catch (PrestoException exception) {
            assertTrue(exception.getMessage().contains(expErrorMessage), exception.getMessage());
        }
    }
}
