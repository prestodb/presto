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
package com.facebook.presto.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.makeCompatibleName;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Tests for ParquetTypeUtils.makeCompatibleName() with hyphenated struct field names.
 *
 * Validates the fix for reading Parquet files where struct field names contain
 * special characters (e.g., "aws-region") that are hex-encoded by the writer
 * (e.g., "aws_x2Dregion") but were looked up using raw names by the reader,
 * causing NULL results before the fix.
 */
public class TestParquetTypeUtils
{
    /**
     * Test makeCompatibleName() hex-encoding for common special characters.
     */
    @Test
    public void testMakeCompatibleName()
    {
        // Hyphens (most common case)
        assertEquals(makeCompatibleName("aws-region"), "aws_x2Dregion");
        assertEquals(makeCompatibleName("user-id"), "user_x2Did");

        // Dots
        assertEquals(makeCompatibleName("user.name"), "user_x2Ename");

        // Spaces
        assertEquals(makeCompatibleName("field name"), "field_x20name");

        // Leading digits
        assertEquals(makeCompatibleName("1field"), "_1field");

        // Multiple special characters
        assertEquals(makeCompatibleName("field-one.two"), "field_x2Done_x2Etwo");

        // Valid names unchanged
        assertEquals(makeCompatibleName("normal_field"), "normal_field");
        assertEquals(makeCompatibleName("fieldName"), "fieldName");

        // Already encoded (no double encoding)
        assertEquals(makeCompatibleName("aws_x2Dregion"), "aws_x2Dregion");
    }

    /**
     * Test reading a pre-canned Parquet file with hyphenated struct field names.
     * This file contains a struct with hex-encoded field names and corner cases:
     * - aws_x2Dregion (from "aws-region")
     * - user_x2Did (from "user-id")
     * - data_x2Dcenter (from "data-center")
     * - aws_x2Dregion_2 (corner case: already encoded name as field)
     * - normal_field (no encoding needed)
     */
    @Test
    public void testReadPreCannedParquetWithHyphenatedFields()
            throws IOException
    {
        Path path = new Path(requireNonNull(
                getClass().getClassLoader()
                        .getResource("hyphenated-fields/hyphenated_struct_fields.parquet"))
                .toString());
        Configuration conf = new Configuration(false);

        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, path);
        MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();

        // Verify the schema structure
        assertNotNull(fileSchema);
        assertEquals(fileSchema.getFieldCount(), 2); // id and application

        // Get the "application" struct
        GroupType applicationGroup = fileSchema.getType(1).asGroupType();
        assertEquals(applicationGroup.getName(), "application");

        // Verify hex-encoded field names exist in Parquet schema
        assertNotNull(applicationGroup.getType("aws_x2Dregion"));
        assertNotNull(applicationGroup.getType("user_x2Did"));
        assertNotNull(applicationGroup.getType("data_x2Dcenter"));
        assertNotNull(applicationGroup.getType("aws_x2Dregion_2"));
        assertNotNull(applicationGroup.getType("normal_field"));

        // Verify makeCompatibleName() produces matching encoded names
        assertEquals(makeCompatibleName("aws-region"), "aws_x2Dregion");
        assertEquals(makeCompatibleName("user-id"), "user_x2Did");
        assertEquals(makeCompatibleName("data-center"), "data_x2Dcenter");
    }

    /**
     * Test that lookupColumnByName() finds fields when using makeCompatibleName().
     * This is what ColumnIOConverter.constructField does after the fix.
     */
    @Test
    public void testLookupWithMakeCompatibleNameFindsFields()
            throws IOException
    {
        Path path = new Path(requireNonNull(
                getClass().getClassLoader()
                        .getResource("hyphenated-fields/hyphenated_struct_fields.parquet"))
                .toString());
        Configuration conf = new Configuration(false);

        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, path);
        MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = new ColumnIOFactory().getColumnIO(fileSchema);

        // Get the "application" struct ColumnIO
        org.apache.parquet.io.GroupColumnIO applicationColumnIO =
                (org.apache.parquet.io.GroupColumnIO) messageColumnIO.getChild(1);

        // WITH makeCompatibleName() - fields should be found and names should match
        assertEquals(requireNonNull(lookupColumnByName(applicationColumnIO, makeCompatibleName("aws-region"))).getName(), "aws_x2Dregion");
        assertEquals(requireNonNull(lookupColumnByName(applicationColumnIO, makeCompatibleName("user-id"))).getName(), "user_x2Did");
        assertEquals(requireNonNull(lookupColumnByName(applicationColumnIO, makeCompatibleName("data-center"))).getName(), "data_x2Dcenter");
        assertEquals(requireNonNull(lookupColumnByName(applicationColumnIO, makeCompatibleName("normal_field"))).getName(), "normal_field");
        assertEquals(requireNonNull(lookupColumnByName(applicationColumnIO, makeCompatibleName("aws_x2Dregion_2"))).getName(), "aws_x2Dregion_2");
    }

    /**
     * Test that lookupColumnByName() does NOT find fields without makeCompatibleName().
     * This proves the bug - before the fix, raw names were used and returned null.
     */
    @Test
    public void testLookupWithoutEncodingDoesNotFindFields()
            throws IOException
    {
        Path path = new Path(requireNonNull(
                getClass().getClassLoader()
                        .getResource("hyphenated-fields/hyphenated_struct_fields.parquet"))
                .toString());
        Configuration conf = new Configuration(false);

        ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, path);
        MessageType fileSchema = parquetMetadata.getFileMetaData().getSchema();
        MessageColumnIO messageColumnIO = new ColumnIOFactory().getColumnIO(fileSchema);

        // Get the "application" struct ColumnIO
        org.apache.parquet.io.GroupColumnIO applicationColumnIO =
                (org.apache.parquet.io.GroupColumnIO) messageColumnIO.getChild(1);

        // WITHOUT makeCompatibleName() - fields should NOT be found
        assertNull(lookupColumnByName(applicationColumnIO, "aws-region"));
        assertNull(lookupColumnByName(applicationColumnIO, "user-id"));
        assertNull(lookupColumnByName(applicationColumnIO, "user.name"));
    }
}
