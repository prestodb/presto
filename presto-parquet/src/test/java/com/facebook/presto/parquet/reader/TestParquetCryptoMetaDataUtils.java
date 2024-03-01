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
package com.facebook.presto.parquet.reader;

import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static org.apache.parquet.crypto.ParquetCryptoMetaDataUtils.removeColumnsInSchema;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestParquetCryptoMetaDataUtils
{
    @Test
    public void testRemoveColumnsInSchema()
    {
        MessageType schema = new MessageType("schema",
                new PrimitiveType(REQUIRED, INT64, "DocId"),
                new PrimitiveType(REQUIRED, BINARY, "Name"),
                new PrimitiveType(REQUIRED, BINARY, "Gender"),
                new GroupType(OPTIONAL, "Links",
                        new PrimitiveType(REPEATED, INT64, "Backward"),
                        new PrimitiveType(REPEATED, INT64, "Forward")));
        Set<ColumnPath> paths = new HashSet<>();
        paths.add(ColumnPath.fromDotString("Name"));

        MessageType newSchema = removeColumnsInSchema(schema, paths);

        String[] docId = {"DocId"};
        assertTrue(newSchema.containsPath(docId));
        String[] gender = {"Gender"};
        assertTrue(newSchema.containsPath(gender));
        String[] linkForward = {"Links", "Forward"};
        assertTrue(newSchema.containsPath(linkForward));
        String[] name = {"Name"};
        assertFalse(newSchema.containsPath(name));
    }

    @Test
    public void testRemoveNestedColumnsInSchema()
    {
        MessageType schema = new MessageType("schema",
                new PrimitiveType(REQUIRED, INT64, "DocId"),
                new PrimitiveType(REQUIRED, BINARY, "Name"),
                new PrimitiveType(REQUIRED, BINARY, "Gender"),
                new GroupType(OPTIONAL, "Links",
                        new PrimitiveType(REPEATED, INT64, "Backward"),
                        new PrimitiveType(REPEATED, INT64, "Forward")));
        Set<ColumnPath> paths = new HashSet<>();
        paths.add(ColumnPath.fromDotString("Links.Backward"));

        MessageType newSchema = removeColumnsInSchema(schema, paths);

        String[] docId = {"DocId"};
        assertTrue(newSchema.containsPath(docId));
        String[] gender = {"Gender"};
        assertTrue(newSchema.containsPath(gender));
        String[] name = {"Name"};
        assertTrue(newSchema.containsPath(name));
        String[] linkForward = {"Links", "Forward"};
        assertTrue(newSchema.containsPath(linkForward));
        String[] linkBackward = {"Links", "Backward"};
        assertFalse(newSchema.containsPath(linkBackward));
    }
}
