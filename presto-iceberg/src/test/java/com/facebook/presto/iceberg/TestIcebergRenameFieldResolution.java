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

import com.facebook.presto.iceberg.IcebergAbstractMetadata.ResolvedField;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.iceberg.IcebergAbstractMetadata.resolveFieldToRename;
import static com.facebook.presto.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class TestIcebergRenameFieldResolution
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");

    /**
     * <pre>
     * id        integer
     * s         struct(a integer, b struct(c integer, d integer), solo struct(x integer),
     *                  element integer, key integer, value integer)
     * items     list(struct(name varchar, keep integer, value integer))
     * m         map(varchar, struct(a integer, b integer, key integer))
     * plain     integer
     * MixedCase struct(Inner integer, other integer)
     * Top, top  integer                                      (case collision at the top level)
     * c         struct(Dup integer, dup integer, other integer)  (case collision inside a struct)
     * </pre>
     */
    private static Schema schema()
    {
        Ids ids = new Ids();
        return new Schema(
                field(ids, "id", IntegerType.get()),
                field(ids, "s", StructType.of(
                        field(ids, "a", IntegerType.get()),
                        field(ids, "b", StructType.of(
                                field(ids, "c", IntegerType.get()),
                                field(ids, "d", IntegerType.get()))),
                        field(ids, "solo", StructType.of(
                                field(ids, "x", IntegerType.get()))),
                        field(ids, "element", IntegerType.get()),
                        field(ids, "key", IntegerType.get()),
                        field(ids, "value", IntegerType.get()))),
                field(ids, "items", ListType.ofOptional(ids.next(), StructType.of(
                        field(ids, "name", StringType.get()),
                        field(ids, "keep", IntegerType.get()),
                        field(ids, "value", IntegerType.get())))),
                field(ids, "m", MapType.ofOptional(ids.next(), ids.next(), StringType.get(), StructType.of(
                        field(ids, "a", IntegerType.get()),
                        field(ids, "b", IntegerType.get()),
                        field(ids, "key", IntegerType.get())))),
                field(ids, "plain", IntegerType.get()),
                field(ids, "MixedCase", StructType.of(
                        field(ids, "Inner", IntegerType.get()),
                        field(ids, "other", IntegerType.get()))),
                field(ids, "Top", IntegerType.get()),
                field(ids, "top", IntegerType.get()),
                field(ids, "c", StructType.of(
                        field(ids, "Dup", IntegerType.get()),
                        field(ids, "dup", IntegerType.get()),
                        field(ids, "other", IntegerType.get()))));
    }

    @Test
    public void testExactAndCaseInsensitiveMatch()
    {
        Schema schema = schema();
        assertResolves(schema, "s.a", "s.a");
        assertResolves(schema, "s.b.c", "s.b.c");
        assertResolves(schema, "S.B.C", "s.b.c");
        assertResolves(schema, "S.B", "s.b");
        // the canonical path keeps the spelling stored in the schema, not the spelling of the request
        assertResolves(schema, "mixedcase.inner", "MixedCase.Inner");
        assertResolves(schema, "MIXEDCASE.INNER", "MixedCase.Inner");
        assertResolves(schema, "MixedCase.Inner", "MixedCase.Inner");
    }

    @Test
    public void testContainingStruct()
    {
        Schema schema = schema();
        ResolvedField resolved = resolveFieldToRename(schema, path("s.b.c"), TABLE, false).get();
        assertEquals(resolved.field.name(), "c");
        assertEquals(resolved.containingStruct.fields().size(), 2);
        assertEquals(resolved.containingStruct.fields().get(0).name(), "c");
        assertEquals(resolved.containingStruct.fields().get(1).name(), "d");
    }

    @Test
    public void testAmbiguousCaseInsensitiveMatch()
    {
        Schema schema = schema();
        // an exact match wins over a case-insensitive one
        assertResolves(schema, "c.Dup", "c.Dup");
        assertResolves(schema, "c.dup", "c.dup");

        assertFails(schema, "c.DUP", NOT_SUPPORTED,
                "Field 'c.DUP' in table 'schema.table' is ambiguous: multiple fields match ignoring case");
        assertFails(schema, "TOP.x", NOT_SUPPORTED,
                "Field 'TOP' in table 'schema.table' is ambiguous: multiple fields match ignoring case");
    }

    @Test
    public void testCaseCollisionElsewhereDoesNotAffectLookup()
    {
        Schema schema = schema();
        // Schema.caseInsensitiveFindField fails for every name once the schema has a case collision
        assertResolves(schema, "s.a", "s.a");
        assertResolves(schema, "S.B.D", "s.b.d");
        assertResolves(schema, "c.other", "c.other");
        assertResolves(schema, "C.OTHER", "c.other");
    }

    @Test
    public void testStructFieldsNamedLikeCollectionSteps()
    {
        Schema schema = schema();
        // in a struct, element/key/value are ordinary field names
        assertResolves(schema, "s.element", "s.element");
        assertResolves(schema, "s.key", "s.key");
        assertResolves(schema, "s.value", "s.value");
        assertResolves(schema, "S.VALUE", "s.value");
    }

    @Test
    public void testMissingFieldThrowsColumnNotFound()
    {
        Schema schema = schema();
        assertFails(schema, "s.nonexistent", COLUMN_NOT_FOUND,
                "Field 's.nonexistent' does not exist in table 'schema.table'");
        assertFails(schema, "s.b.missing", COLUMN_NOT_FOUND,
                "Field 's.b.missing' does not exist in table 'schema.table'");
    }

    @Test
    public void testMissingParentThrowsColumnNotFound()
    {
        Schema schema = schema();
        assertFails(schema, "nonexistent.a", COLUMN_NOT_FOUND,
                "Cannot find parent field 'nonexistent' in table 'schema.table'");
        assertFails(schema, "s.nonexistent.a", COLUMN_NOT_FOUND,
                "Cannot find parent field 's.nonexistent' in table 'schema.table'");
    }

    @Test
    public void testNonStructParentThrowsNotSupported()
    {
        Schema schema = schema();
        assertFails(schema, "plain.child", NOT_SUPPORTED,
                "Parent field 'plain' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "s.a.child", NOT_SUPPORTED,
                "Parent field 's.a' in table 'schema.table' is not a struct/ROW type");
    }

    @Test
    public void testListAndMapTraversalThrowsNotSupported()
    {
        Schema schema = schema();
        assertFails(schema, "items.element.name", NOT_SUPPORTED,
                "Parent field 'items' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "m.value.a", NOT_SUPPORTED,
                "Parent field 'm' in table 'schema.table' is not a struct/ROW type");
    }

    private static void assertResolves(Schema schema, String path, String expectedCanonicalPath)
    {
        ResolvedField resolved = resolveFieldToRename(schema, path(path), TABLE, false).get();
        assertEquals(schema.findColumnName(resolved.field.fieldId()), expectedCanonicalPath);
    }

    private static void assertFails(Schema schema, String path, ErrorCodeSupplier expectedErrorCode, String expectedMessage)
    {
        PrestoException e = expectThrows(PrestoException.class,
                () -> resolveFieldToRename(schema, path(path), TABLE, false));
        assertEquals(e.getErrorCode(), expectedErrorCode.toErrorCode(),
                "error code mismatch for path " + path + ": " + e.getMessage());
        assertEquals(e.getMessage(), expectedMessage);
    }

    private static List<String> path(String dotted)
    {
        return ImmutableList.copyOf(Splitter.on('.').split(dotted));
    }

    private static NestedField field(Ids ids, String name, Type type)
    {
        return NestedField.optional(ids.next(), name, type);
    }

    private static final class Ids
    {
        private int next = 1;

        int next()
        {
            return next++;
        }
    }
}
