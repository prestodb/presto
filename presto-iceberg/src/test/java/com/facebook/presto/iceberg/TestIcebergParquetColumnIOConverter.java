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
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.VariantField;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link IcebergParquetColumnIOConverter#constructField} focusing on the
 * Variant (JSON) branch — specifically:
 * <ol>
 *   <li>Reversed physical child ordering ({@code metadata} before {@code value}) is handled
 *       correctly by name-based lookup rather than positional index.</li>
 *   <li>A file with a missing {@code value} or {@code metadata} child throws an informative
 *       {@link IllegalArgumentException}.</li>
 *   <li>A shredded-Variant file that carries {@code typed_value} throws an explicit
 *       {@link IllegalArgumentException} rather than silently returning {@code null}.</li>
 * </ol>
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
}
