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

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.hive.DwrfEncryptionMetadata.TABLE_IDENTIFIER;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static org.testng.Assert.assertEquals;

public class TestDwrfEncryptionMetadata
{
    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Cannot have both table and column level settings. Given: \\[__TABLE__, foo\\]")
    public void testOnlyOneTableProperty()
    {
        new DwrfEncryptionMetadata(ImmutableMap.of(TABLE_IDENTIFIER, "abcd".getBytes(), "foo", "def".getBytes()), ImmutableMap.of(), "", "");
    }

    @Test
    public void testToKeyMap()
    {
        DwrfEncryptionMetadata dwrfEncryptionMetadata = new DwrfEncryptionMetadata(
                ImmutableMap.of("c1", "abcd".getBytes(),
                        "c3.d2.e1.f1", "def".getBytes(),
                        "c3.d2.e2", "ghi".getBytes()),
                ImmutableMap.of(),
                "test_algo",
                "test_provider");

        List<HiveColumnHandle> columnHandleList = ImmutableList.of(
                new HiveColumnHandle("c1", HIVE_INT, TypeSignature.parseTypeSignature(BIGINT), 0, HiveColumnHandle.ColumnType.REGULAR, Optional.empty(), Optional.empty()),
                new HiveColumnHandle("c3", HIVE_INT, TypeSignature.parseTypeSignature(BIGINT), 2, HiveColumnHandle.ColumnType.REGULAR, Optional.empty(), Optional.empty()));

        List<OrcType> orcTypes = ImmutableList.of(
                new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(1, 2, 4), ImmutableList.of("c1", "c2,", "c3"), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(3), ImmutableList.of("d1"), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(5, 6), ImmutableList.of("d1", "d2"), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(7, 9), ImmutableList.of("e1", "e2"), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(8), ImmutableList.of("f1"), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()));
        Map<Integer, Slice> actualKeyMap = dwrfEncryptionMetadata.toKeyMap(orcTypes, columnHandleList);
        Map<Integer, Slice> expectedKeyMap = ImmutableMap.of(
                1, Slices.wrappedBuffer("abcd".getBytes()),
                8, Slices.wrappedBuffer("def".getBytes()),
                9, Slices.wrappedBuffer("ghi".getBytes()));
        assertEquals(actualKeyMap, expectedKeyMap);
    }

    @Test
    public void testWholeTable()
    {
        DwrfEncryptionMetadata dwrfEncryptionMetadata = new DwrfEncryptionMetadata(
                ImmutableMap.of(TABLE_IDENTIFIER, "abcd".getBytes()),
                ImmutableMap.of(),
                "test_algo",
                "test_provider");

        List<HiveColumnHandle> columnHandleList = ImmutableList.of(
                new HiveColumnHandle("c1", HIVE_INT, TypeSignature.parseTypeSignature(BIGINT), 0, HiveColumnHandle.ColumnType.REGULAR, Optional.empty(), Optional.empty()),
                new HiveColumnHandle("c2", HIVE_INT, TypeSignature.parseTypeSignature(BIGINT), 2, HiveColumnHandle.ColumnType.REGULAR, Optional.empty(), Optional.empty()));

        List<OrcType> orcTypes = ImmutableList.of(
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()));
        Map<Integer, Slice> actualKeyMap = dwrfEncryptionMetadata.toKeyMap(orcTypes, columnHandleList);
        Map<Integer, Slice> expectedKeyMap = ImmutableMap.of(0, Slices.wrappedBuffer("abcd".getBytes()));
        assertEquals(actualKeyMap, expectedKeyMap);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidKeyMap()
    {
        DwrfEncryptionMetadata dwrfEncryptionMetadata = new DwrfEncryptionMetadata(ImmutableMap.of("c1", "abcd".getBytes()), ImmutableMap.of(), "test_algo", "test_provider");

        List<HiveColumnHandle> columnHandleList = ImmutableList.of(
                new HiveColumnHandle("column1", HIVE_INT, TypeSignature.parseTypeSignature(BIGINT), 0, HiveColumnHandle.ColumnType.REGULAR, Optional.empty(), Optional.empty()));

        List<OrcType> orcTypes = ImmutableList.of(
                new OrcType(OrcType.OrcTypeKind.STRUCT, ImmutableList.of(1), ImmutableList.of("column1"), Optional.empty(), Optional.empty(), Optional.empty()),
                new OrcType(OrcType.OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()));
        dwrfEncryptionMetadata.toKeyMap(orcTypes, columnHandleList);
    }
}
