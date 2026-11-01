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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.MAP;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static com.facebook.presto.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

/**
 * Unit tests for {@link IcebergPageSourceProvider#toPhysicalOrcType}.
 *
 * <p>{@code toPhysicalOrcType} rewrites a logical Presto type by substituting each nested ROW
 * field name with the physical ORC stream name that carries the corresponding Iceberg field ID.
 * This enables reading historical ORC files whose sub-field streams still carry pre-rename names.
 *
 * <p>These tests cover the ARRAY and MAP container-recursion branches added to support ROW types
 * nested inside collection containers.
 */
public class TestToPhysicalOrcType
{
    private static final FunctionAndTypeManager TYPE_MANAGER = createTestFunctionAndTypeManager();

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Builds an INT OrcType with no attributes (used as leaf primitive placeholder). */
    private static OrcType intType()
    {
        return new OrcType(OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** Builds an INT OrcType carrying the given Iceberg field ID as an attribute. */
    private static OrcType intTypeWithId(int icebergId)
    {
        return new OrcType(OrcTypeKind.INT, ImmutableList.of(), ImmutableList.of(),
                Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableMap.of(ORC_ICEBERG_ID_KEY, String.valueOf(icebergId)));
    }

    /**
     * Builds a STRUCT OrcType with one field at subTypeIndex named physicalFieldName.
     * The sub-type at subTypeIndex must carry the iceberg.id attribute separately.
     */
    private static OrcType structType(int subTypeIndex, String physicalFieldName)
    {
        return new OrcType(OrcTypeKind.STRUCT, ImmutableList.of(subTypeIndex), ImmutableList.of(physicalFieldName),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** Like {@link #structType} but the STRUCT itself carries an Iceberg field ID (a ROW field that is itself a ROW). */
    private static OrcType structTypeWithId(int subTypeIndex, String physicalFieldName, int icebergId)
    {
        return new OrcType(OrcTypeKind.STRUCT, ImmutableList.of(subTypeIndex), ImmutableList.of(physicalFieldName),
                Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableMap.of(ORC_ICEBERG_ID_KEY, String.valueOf(icebergId)));
    }

    /** Builds a LIST OrcType whose single element type is at elementTypeIndex. */
    private static OrcType listType(int elementTypeIndex)
    {
        return new OrcType(OrcTypeKind.LIST, ImmutableList.of(elementTypeIndex), ImmutableList.of("item"),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    /** Builds a MAP OrcType with key at keyTypeIndex and value at valueTypeIndex. */
    private static OrcType mapType(int keyTypeIndex, int valueTypeIndex)
    {
        return new OrcType(OrcTypeKind.MAP, ImmutableList.of(keyTypeIndex, valueTypeIndex), ImmutableList.of("key", "value"),
                Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static ColumnIdentity primitiveIdentity(int id, String name)
    {
        return new ColumnIdentity(id, name, PRIMITIVE, ImmutableList.of());
    }

    private static ColumnIdentity structIdentity(String name, List<ColumnIdentity> children)
    {
        return new ColumnIdentity(0, name, STRUCT, children);
    }

    private static ColumnIdentity structIdentity(int id, String name, List<ColumnIdentity> children)
    {
        return new ColumnIdentity(id, name, STRUCT, children);
    }

    private static MapType mapOf(Type keyType, Type valueType)
    {
        return (MapType) TYPE_MANAGER.getParameterizedType(
                StandardTypes.MAP,
                ImmutableList.of(
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(valueType.getTypeSignature())));
    }

    private static ColumnIdentity arrayIdentity(String name, ColumnIdentity elementIdentity)
    {
        return new ColumnIdentity(0, name, ARRAY, ImmutableList.of(elementIdentity));
    }

    private static ColumnIdentity mapIdentity(String name, ColumnIdentity keyIdentity, ColumnIdentity valueIdentity)
    {
        return new ColumnIdentity(0, name, MAP, ImmutableList.of(keyIdentity, valueIdentity));
    }

    // -----------------------------------------------------------------------
    // ARRAY<ROW> tests
    // -----------------------------------------------------------------------

    /**
     * ARRAY(ROW(new_name BIGINT)) where the physical ORC file stores the element struct
     * field as "old_name" (with iceberg.id = 10). The method must recurse into the array
     * element and rewrite the ROW field name from "new_name" to "old_name".
     *
     * <p>ORC type list layout:
     * <pre>
     *   [0] LIST    → element at index 1
     *   [1] STRUCT  → field "old_name" at index 2
     *   [2] INT     with iceberg.id = 10
     * </pre>
     */
    @Test
    public void testArrayWithRenamedRowElement()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                listType(1),
                structType(2, "old_name"),
                intTypeWithId(10));

        RowType logicalElementType = RowType.from(ImmutableList.of(RowType.field("new_name", BIGINT)));
        ArrayType logicalArrayType = new ArrayType(logicalElementType);

        ColumnIdentity elementStructIdentity = structIdentity("element",
                ImmutableList.of(primitiveIdentity(10, "new_name")));
        ColumnIdentity arrayColumnIdentity = arrayIdentity("col", elementStructIdentity);

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalArrayType, arrayColumnIdentity, orcTypes, 0);

        // The element ROW must now use the physical name "old_name".
        ArrayType expected = new ArrayType(RowType.from(ImmutableList.of(RowType.field("old_name", BIGINT))));
        assertEquals(result, expected);
    }

    /**
     * ARRAY(BIGINT): when the element type is a non-struct primitive, the array must be
     * returned unchanged (same reference, no allocation).
     */
    @Test
    public void testArrayWithPrimitiveElementUnchanged()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                listType(1),
                intType());

        ArrayType logicalArrayType = new ArrayType(BIGINT);
        ColumnIdentity arrayColumnIdentity = arrayIdentity("col", primitiveIdentity(5, "item"));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalArrayType, arrayColumnIdentity, orcTypes, 0);

        assertSame(result, logicalArrayType, "primitive element array must be returned unchanged");
    }

    // -----------------------------------------------------------------------
    // MAP<key, ROW> tests
    // -----------------------------------------------------------------------

    /**
     * MAP(INTEGER, ROW(new_val BIGINT)) where the physical ORC file stores the map value
     * struct field as "old_val" (with iceberg.id = 20). The key type (INTEGER) is a
     * primitive and must pass through unchanged.
     *
     * <p>ORC type list layout:
     * <pre>
     *   [0] MAP     → key at index 1, value at index 2
     *   [1] INT     (key, no iceberg.id needed for primitives)
     *   [2] STRUCT  → field "old_val" at index 3
     *   [3] INT     with iceberg.id = 20
     * </pre>
     */
    @Test
    public void testMapWithRenamedRowValue()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                mapType(1, 2),
                intType(),                  // key
                structType(3, "old_val"),   // value struct
                intTypeWithId(20));         // value struct field

        com.facebook.presto.common.type.MapType logicalMapType =
                (com.facebook.presto.common.type.MapType) TYPE_MANAGER.getParameterizedType(
                        StandardTypes.MAP,
                        ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(RowType.from(
                                        ImmutableList.of(RowType.field("new_val", BIGINT))).getTypeSignature())));

        ColumnIdentity keyIdentity = primitiveIdentity(30, "key");
        ColumnIdentity valueStructIdentity = structIdentity("value",
                ImmutableList.of(primitiveIdentity(20, "new_val")));
        ColumnIdentity mapColumnIdentity = mapIdentity("col", keyIdentity, valueStructIdentity);

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalMapType, mapColumnIdentity, orcTypes, 0);

        // Value ROW must carry the physical name "old_val"; key type is unchanged.
        com.facebook.presto.common.type.MapType expected =
                (com.facebook.presto.common.type.MapType) TYPE_MANAGER.getParameterizedType(
                        StandardTypes.MAP,
                        ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(RowType.from(
                                        ImmutableList.of(RowType.field("old_val", BIGINT))).getTypeSignature())));
        assertEquals(result, expected);
    }

    /**
     * MAP(INTEGER, BIGINT): both key and value are primitives; expect the same object returned.
     */
    @Test
    public void testMapWithPrimitiveKeyAndValueUnchanged()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                mapType(1, 2),
                intType(),
                intType());

        com.facebook.presto.common.type.MapType logicalMapType =
                (com.facebook.presto.common.type.MapType) TYPE_MANAGER.getParameterizedType(
                        StandardTypes.MAP,
                        ImmutableList.of(
                                TypeSignatureParameter.of(INTEGER.getTypeSignature()),
                                TypeSignatureParameter.of(BIGINT.getTypeSignature())));

        ColumnIdentity mapColumnIdentity = mapIdentity("col",
                primitiveIdentity(1, "key"),
                primitiveIdentity(2, "value"));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalMapType, mapColumnIdentity, orcTypes, 0);

        assertSame(result, logicalMapType, "primitive-valued map must be returned unchanged");
    }

    // -----------------------------------------------------------------------
    // Deeper and less common nesting
    // -----------------------------------------------------------------------

    /**
     * MAP(INTEGER, ARRAY(ROW(new_name BIGINT))): the map value is itself a container, so the MAP
     * branch must delegate into the ARRAY branch and on into the element ROW.
     *
     * <pre>
     *   [0] MAP     → key at index 1, value at index 2
     *   [1] INT     (key)
     *   [2] LIST    → element at index 3
     *   [3] STRUCT  → field "old_name" at index 4
     *   [4] INT     with iceberg.id = 10
     * </pre>
     */
    @Test
    public void testMapWithArrayOfRenamedRowValue()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                mapType(1, 2),
                intType(),
                listType(3),
                structType(4, "old_name"),
                intTypeWithId(10));

        MapType logicalType = mapOf(INTEGER, new ArrayType(RowType.from(ImmutableList.of(RowType.field("new_name", BIGINT)))));
        ColumnIdentity identity = mapIdentity("col",
                primitiveIdentity(30, "key"),
                arrayIdentity("value", structIdentity("element", ImmutableList.of(primitiveIdentity(10, "new_name")))));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalType, identity, orcTypes, 0);

        assertEquals(result, mapOf(INTEGER, new ArrayType(RowType.from(ImmutableList.of(RowType.field("old_name", BIGINT))))));
    }

    /**
     * ARRAY(ROW(new_outer ROW(new_inner BIGINT))): both ROW levels were renamed, so the recursion
     * must continue through the element ROW into the ROW-typed field and rewrite both names.
     *
     * <pre>
     *   [0] LIST    → element at index 1
     *   [1] STRUCT  → field "old_outer" at index 2
     *   [2] STRUCT  with iceberg.id = 10 → field "old_inner" at index 3
     *   [3] INT     with iceberg.id = 11
     * </pre>
     */
    @Test
    public void testArrayWithRenamedRowNestedInRow()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                listType(1),
                structType(2, "old_outer"),
                structTypeWithId(3, "old_inner", 10),
                intTypeWithId(11));

        RowType innerRow = RowType.from(ImmutableList.of(RowType.field("new_inner", BIGINT)));
        ArrayType logicalArrayType = new ArrayType(RowType.from(ImmutableList.of(RowType.field("new_outer", innerRow))));

        ColumnIdentity innerIdentity = structIdentity(10, "new_outer",
                ImmutableList.of(primitiveIdentity(11, "new_inner")));
        ColumnIdentity arrayColumnIdentity = arrayIdentity("col",
                structIdentity("element", ImmutableList.of(innerIdentity)));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalArrayType, arrayColumnIdentity, orcTypes, 0);

        RowType expectedInner = RowType.from(ImmutableList.of(RowType.field("old_inner", BIGINT)));
        assertEquals(result, new ArrayType(RowType.from(ImmutableList.of(RowType.field("old_outer", expectedInner)))));
    }

    /**
     * ARRAY(ARRAY(ROW(new_name BIGINT))): a container nested directly in a container.
     *
     * <pre>
     *   [0] LIST    → element at index 1
     *   [1] LIST    → element at index 2
     *   [2] STRUCT  → field "old_name" at index 3
     *   [3] INT     with iceberg.id = 10
     * </pre>
     */
    @Test
    public void testArrayOfArrayWithRenamedRowElement()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                listType(1),
                listType(2),
                structType(3, "old_name"),
                intTypeWithId(10));

        ArrayType logicalType = new ArrayType(new ArrayType(RowType.from(ImmutableList.of(RowType.field("new_name", BIGINT)))));
        ColumnIdentity identity = arrayIdentity("col",
                arrayIdentity("element", structIdentity("element", ImmutableList.of(primitiveIdentity(10, "new_name")))));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalType, identity, orcTypes, 0);

        assertEquals(result, new ArrayType(new ArrayType(RowType.from(ImmutableList.of(RowType.field("old_name", BIGINT))))));
    }

    /**
     * A ROW field inside an ARRAY element whose Iceberg ID is absent from the file (added after the
     * file was written) must get the synthetic missing name rather than binding by logical name,
     * while the field that is present is still renamed to its physical name.
     */
    @Test
    public void testArrayRowFieldMissingFromFileGetsSyntheticName()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                listType(1),
                structType(2, "old_a"),
                intTypeWithId(10));

        ArrayType logicalType = new ArrayType(RowType.from(ImmutableList.of(
                RowType.field("a", BIGINT),
                RowType.field("b", BIGINT))));
        ColumnIdentity identity = arrayIdentity("col", structIdentity("element",
                ImmutableList.of(primitiveIdentity(10, "a"), primitiveIdentity(11, "b"))));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalType, identity, orcTypes, 0);

        assertEquals(result, new ArrayType(RowType.from(ImmutableList.of(
                RowType.field("old_a", BIGINT),
                RowType.field("$missing_orc_field$11", BIGINT)))));
    }

    // -----------------------------------------------------------------------
    // Identity-count mismatch early returns
    // -----------------------------------------------------------------------

    /**
     * An ARRAY identity must have exactly one child. With two, the element cannot be paired with an
     * identity, so the type is returned unchanged even though the element ROW would otherwise be renamed.
     */
    @Test
    public void testArrayWithMismatchedIdentityCountUnchanged()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                listType(1),
                structType(2, "old_name"),
                intTypeWithId(10));

        ArrayType logicalType = new ArrayType(RowType.from(ImmutableList.of(RowType.field("new_name", BIGINT))));
        ColumnIdentity identity = new ColumnIdentity(0, "col", ARRAY, ImmutableList.of(
                structIdentity("element", ImmutableList.of(primitiveIdentity(10, "new_name"))),
                primitiveIdentity(99, "extra")));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalType, identity, orcTypes, 0);

        assertSame(result, logicalType, "array with mismatched identity count must be returned unchanged");
    }

    /**
     * A MAP identity must have exactly two children (key, value). With three, the type is returned
     * unchanged even though the value ROW would otherwise be renamed.
     */
    @Test
    public void testMapWithMismatchedIdentityCountUnchanged()
    {
        List<OrcType> orcTypes = ImmutableList.of(
                mapType(1, 2),
                intType(),
                structType(3, "old_val"),
                intTypeWithId(20));

        MapType logicalType = mapOf(INTEGER, RowType.from(ImmutableList.of(RowType.field("new_val", BIGINT))));
        ColumnIdentity identity = new ColumnIdentity(0, "col", MAP, ImmutableList.of(
                primitiveIdentity(30, "key"),
                structIdentity("value", ImmutableList.of(primitiveIdentity(20, "new_val"))),
                primitiveIdentity(99, "extra")));

        Type result = IcebergPageSourceProvider.toPhysicalOrcType(logicalType, identity, orcTypes, 0);

        assertSame(result, logicalType, "map with mismatched identity count must be returned unchanged");
    }
}
