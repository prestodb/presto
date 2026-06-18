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
package com.facebook.presto.hive;

import com.facebook.presto.hive.ColumnEncryptionInformation.ColumnWithStructSubfield;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.ColumnEncryptionInformation.fromHiveProperty;
import static com.facebook.presto.hive.ColumnEncryptionInformation.fromTableProperty;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestColumnEncryptionInformation
{
    @Test
    public void testFromTablePropertyNullInput()
    {
        ColumnEncryptionInformation encryptionInformation = fromTableProperty(null);
        assertFalse(encryptionInformation.hasEntries());
    }

    @Test
    public void testFromTablePropertyValidInput()
    {
        ColumnEncryptionInformation encryptionInformation = fromTableProperty(ImmutableList.of("key1: col1, col2", "key2: col4, col5.a.b"));
        Map<ColumnWithStructSubfield, String> keyReferences = encryptionInformation.getColumnToKeyReference();

        Map<ColumnWithStructSubfield, String> expectedKeyReferences = ImmutableMap.of(
                ColumnWithStructSubfield.valueOf("col1"), "key1",
                ColumnWithStructSubfield.valueOf("col2"), "key1",
                ColumnWithStructSubfield.valueOf("col4"), "key2",
                ColumnWithStructSubfield.valueOf("col5.a.b"), "key2");

        assertEquals(keyReferences, expectedKeyReferences);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Encrypted columns property cannot have null value")
    public void testFromTablePropertyNullListEntries()
    {
        List<String> entries = new ArrayList<>();
        entries.add(null);

        fromTableProperty(entries);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Column col1 has been assigned 2 key references \\(key2 and key1\\). Only 1 is allowed")
    public void testFromTablePropertyDuplicateKeys()
    {
        fromTableProperty(ImmutableList.of("key1:col1", "key2:col1"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Encrypted column entry needs to be in the format 'key1:col1,col2'. Received: key1")
    public void testFromTablePropertyNoColumnsSpecified()
    {
        fromTableProperty(ImmutableList.of("key1"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Encrypted column entry needs to be in the format 'key1:col1,col2'. Received: key1:key2:col1")
    public void testFromTablePropertyMultipleKeys()
    {
        fromTableProperty(ImmutableList.of("key1:key2:col1"));
    }

    @Test
    public void testToTableProperty()
    {
        List<String> originalEntries = ImmutableList.of("key1:col1,col2", "key2:col3,col4");
        ColumnEncryptionInformation encryptionInformation = fromTableProperty(originalEntries);
        List<String> tablePropertyFormat = encryptionInformation.toTableProperty();
        ColumnEncryptionInformation encryptionInformationReconstructed = fromTableProperty(tablePropertyFormat);

        assertEquals(encryptionInformationReconstructed.getColumnToKeyReference(), encryptionInformation.getColumnToKeyReference());
    }

    @Test
    public void testFromHivePropertyNullInput()
    {
        ColumnEncryptionInformation encryptionInformation = fromHiveProperty(null);
        assertFalse(encryptionInformation.hasEntries());
    }

    @Test
    public void testFromHivePropertyValidInput()
    {
        ColumnEncryptionInformation encryptionInformation = fromHiveProperty("key1: col1, col2;key2: col4, col5.a.b");
        Map<ColumnWithStructSubfield, String> keyReferences = encryptionInformation.getColumnToKeyReference();

        Map<ColumnWithStructSubfield, String> expectedKeyReferences = ImmutableMap.of(
                ColumnWithStructSubfield.valueOf("col1"), "key1",
                ColumnWithStructSubfield.valueOf("col2"), "key1",
                ColumnWithStructSubfield.valueOf("col4"), "key2",
                ColumnWithStructSubfield.valueOf("col5.a.b"), "key2");

        assertEquals(keyReferences, expectedKeyReferences);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Column col1 has been assigned 2 key references \\(key2 and key1\\). Only 1 is allowed")
    public void testFromHivePropertyDuplicateKeys()
    {
        fromHiveProperty("key1:col1;key2:col1");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Encrypted column entry needs to be in the format 'key1:col1,col2'. Received: key1")
    public void testFromHivePropertyNoColumnsSpecified()
    {
        fromHiveProperty("key1;key2:col1");
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Encrypted column entry needs to be in the format 'key1:col1,col2'. Received: key1:key2:col1")
    public void testFromHivePropertyMultipleKeys()
    {
        fromHiveProperty("key1:key2:col1");
    }

    @Test
    public void testToHiveProperty()
    {
        String originalInput = "key1:col1,col2;key2:col3,col4";
        ColumnEncryptionInformation encryptionInformation = fromHiveProperty(originalInput);
        String hivePropertyFormat = encryptionInformation.toHiveProperty();
        ColumnEncryptionInformation encryptionInformationReconstructed = fromHiveProperty(hivePropertyFormat);

        assertEquals(encryptionInformationReconstructed.getColumnToKeyReference(), encryptionInformation.getColumnToKeyReference());
    }

    @Test
    public void testColumnWithStructSubfieldWithNoSubfield()
    {
        ColumnWithStructSubfield column = ColumnWithStructSubfield.valueOf("a");
        assertEquals(column.getColumnName(), "a");
        assertFalse(column.getSubfieldPath().isPresent());
        assertFalse(column.getChildField().isPresent());
    }

    @Test
    public void testColumnWithStructSubfieldWithSubfield()
    {
        ColumnWithStructSubfield column = ColumnWithStructSubfield.valueOf("a.b.c.d");
        assertEquals(column.getColumnName(), "a");
        assertTrue(column.getSubfieldPath().isPresent());
        assertEquals(column.getSubfieldPath().get(), "b.c.d");
        assertTrue(column.getChildField().isPresent());
        assertEquals(column.getChildField().get(), ColumnWithStructSubfield.valueOf("b.c.d"));
    }
}
