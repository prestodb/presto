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

import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.VariantField;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link IcebergParquetColumnIOConverter#constructField}.
 *
 * <p>Variant (JSON) branch:
 * <ol>
 *   <li>Reversed physical child ordering ({@code metadata} before {@code value}) is handled
 *       correctly by name-based lookup rather than positional index.</li>
 *   <li>A file with a missing {@code value} or {@code metadata} child throws an informative
 *       {@link IllegalArgumentException}.</li>
 *   <li>A shredded-Variant file that carries {@code typed_value} throws an explicit
 *       {@link IllegalArgumentException} rather than silently returning {@code null}.</li>
 * </ol>
 *
 * <p>ROW branch: a ROW whose Iceberg child-identity count differs from its field count is
 * not resolved, and a ROW whose counts match is resolved by field ID despite a rename.
 */
public class TestIcebergParquetColumnIOConverter
{
    /** A minimal ColumnIdentity for a top-level primitive (the variant column itself). */
    private static ColumnIdentity variantIdentity()
    {
        return new ColumnIdentity(1, "data", PRIMITIVE, ImmutableList.of());
    }

    /**
     * Returns the ColumnIO for the top-level {@code data} group built from {@code schemaDef}.
     */
    private static ColumnIO variantColumnIO(String schemaDef)
    {
        MessageType schema = MessageTypeParser.parseMessageType(schemaDef);
        MessageColumnIO messageIO = getColumnIO(schema, schema);
        return lookupColumnByName(messageIO, "data");
    }

    @Test
    public void testVariantStandardOrdering()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary value; " +
                        "  required binary metadata; " +
                        "} }");

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(
                variantIdentity(), JsonType.JSON, columnIO);

        assertTrue(result.isPresent(), "Expected a VariantField to be returned");
        assertTrue(result.get() instanceof VariantField, "Expected VariantField, got: " + result.get().getClass());
    }

    @Test
    public void testVariantReversedOrdering()
    {
        // metadata listed FIRST — reversed from the standard Iceberg order (Spark/some writers do this).
        // The old positional getChild(0)/getChild(1) would wire the wrong physical columns.
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary metadata; " +
                        "  required binary value; " +
                        "} }");

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(
                variantIdentity(), JsonType.JSON, columnIO);

        assertTrue(result.isPresent(), "Expected a VariantField even with reversed child order");
        assertTrue(result.get() instanceof VariantField,
                "Expected VariantField regardless of physical child ordering, got: " + result.get().getClass());

        VariantField variantField = (VariantField) result.get();

        // Verify via the descriptor path that value→"value" and metadata→"metadata" (not swapped).
        assertTrue(variantField.getValue() instanceof PrimitiveField, "value sub-field must be a PrimitiveField");
        assertTrue(variantField.getMetadata() instanceof PrimitiveField, "metadata sub-field must be a PrimitiveField");

        String[] valuePath = ((PrimitiveField) variantField.getValue()).getDescriptor().getPath();
        String[] metadataPath = ((PrimitiveField) variantField.getMetadata()).getDescriptor().getPath();

        assertEquals(valuePath[valuePath.length - 1], "value",
                "getValue() must be wired to the physical 'value' column, not 'metadata'");
        assertEquals(metadataPath[metadataPath.length - 1], "metadata",
                "getMetadata() must be wired to the physical 'metadata' column, not 'value'");
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Vv]alue.*")
    public void testVariantMissingValueField()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary metadata; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Mm]etadata.*")
    public void testVariantMissingMetadataField()
    {
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary value; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Ss]hredded.*|.*typed_value.*")
    public void testVariantShreddedRejected()
    {
        // Shredded Variant (Iceberg spec §4.3): all three children present — must be rejected
        // explicitly rather than silently returning NULL or reading wrong bytes.
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  required binary metadata; " +
                        "  required binary value; " +
                        "  optional binary typed_value; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*[Ss]hredded.*|.*typed_value.*")
    public void testVariantShreddedRejectedReversedOrder()
    {
        // typed_value must be rejected even when physical child order is non-standard.
        ColumnIO columnIO = variantColumnIO(
                "message table { " +
                        "required group data { " +
                        "  optional binary typed_value; " +
                        "  required binary value; " +
                        "  required binary metadata; " +
                        "} }");

        IcebergParquetColumnIOConverter.constructField(variantIdentity(), JsonType.JSON, columnIO);
    }

    /** The file stores {@code info} with children {@code old_a} (field ID 10) and {@code old_b} (field ID 11). */
    private static ColumnIO infoColumnIO()
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                "message table { " +
                        "required group info { " +
                        "  required int64 old_a = 10; " +
                        "  required int64 old_b = 11; " +
                        "} }");
        MessageColumnIO messageIO = getColumnIO(schema, schema);
        return lookupColumnByName(messageIO, "info");
    }

    @Test
    public void testRowChildIdentityCountMismatchIsNotResolved()
    {
        ColumnIO columnIO = infoColumnIO();
        RowType rowType = RowType.from(ImmutableList.of(RowType.field("a", BIGINT), RowType.field("b", BIGINT)));
        // Two logical fields but only one child identity: positions can no longer be paired by index.
        ColumnIdentity mismatchedIdentity = new ColumnIdentity(1, "info", STRUCT,
                ImmutableList.of(new ColumnIdentity(10, "a", PRIMITIVE, ImmutableList.of())));

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(mismatchedIdentity, rowType, columnIO);

        assertFalse(result.isPresent(), "ROW with mismatched child identity count must not be resolved");
    }

    /**
     * Control for {@link #testRowChildIdentityCountMismatchIsNotResolved}: with matching counts the same
     * file is resolved, and the renamed logical fields {@code a}/{@code b} bind to the physical
     * {@code old_a}/{@code old_b} columns by field ID. This shows the empty result above comes from the
     * count guard and not from a failed lookup.
     */
    @Test
    public void testRowResolvedByFieldIdWhenCountsMatch()
    {
        ColumnIO columnIO = infoColumnIO();
        RowType rowType = RowType.from(ImmutableList.of(RowType.field("a", BIGINT), RowType.field("b", BIGINT)));
        ColumnIdentity identity = new ColumnIdentity(1, "info", STRUCT,
                ImmutableList.of(
                        new ColumnIdentity(10, "a", PRIMITIVE, ImmutableList.of()),
                        new ColumnIdentity(11, "b", PRIMITIVE, ImmutableList.of())));

        Optional<Field> result = IcebergParquetColumnIOConverter.constructField(identity, rowType, columnIO);

        assertTrue(result.isPresent(), "Expected ROW to be resolved when identity count matches field count");
        assertTrue(result.get() instanceof GroupField, "Expected GroupField, got: " + result.get().getClass());
        GroupField group = (GroupField) result.get();
        assertEquals(group.getChildren().size(), 2);
        assertEquals(physicalLeafName(group.getChildren().get(0)), "old_a");
        assertEquals(physicalLeafName(group.getChildren().get(1)), "old_b");
    }

    private static String physicalLeafName(Optional<Field> field)
    {
        assertTrue(field.isPresent() && field.get() instanceof PrimitiveField, "Expected a PrimitiveField child");
        String[] path = ((PrimitiveField) field.get()).getDescriptor().getPath();
        return path[path.length - 1];
    }
}
