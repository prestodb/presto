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

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
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
import java.util.Optional;

import static com.facebook.presto.iceberg.IcebergAbstractMetadata.resolveFieldToDrop;
import static com.facebook.presto.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;

public class TestIcebergDropFieldResolution
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");

    /**
     * <pre>
     * id        integer
     * s         struct(a integer, b struct(c integer, d integer), solo struct(x integer),
     *                  element integer, key integer, value integer)
     * items     list(struct(name varchar, keep integer, value integer))
     * m         map(varchar, struct(a integer, b integer, key integer))
     * ll        list(list(struct(y integer, z integer)))
     * km        map(struct(k1 integer, k2 integer), integer)
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
                field(ids, "ll", ListType.ofOptional(ids.next(), ListType.ofOptional(ids.next(), StructType.of(
                        field(ids, "y", IntegerType.get()),
                        field(ids, "z", IntegerType.get()))))),
                field(ids, "km", MapType.ofOptional(ids.next(), ids.next(), StructType.of(
                        field(ids, "k1", IntegerType.get()),
                        field(ids, "k2", IntegerType.get())), IntegerType.get())),
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
    public void testAmbiguousCaseInsensitiveMatch()
    {
        Schema schema = schema();
        // an exact match wins over a case-insensitive one
        assertResolves(schema, "c.Dup", "c.Dup");
        assertResolves(schema, "c.dup", "c.dup");
        assertResolves(schema, "Top", "Top");
        assertResolves(schema, "top", "top");

        assertFails(schema, "c.DUP", false, NOT_SUPPORTED,
                "Field 'c.DUP' in table 'schema.table' is ambiguous: multiple fields match ignoring case");
        assertFails(schema, "TOP", false, NOT_SUPPORTED,
                "Field 'TOP' in table 'schema.table' is ambiguous: multiple fields match ignoring case");
        // IF EXISTS must not turn an ambiguous request into a silent no-op
        assertFails(schema, "c.DUP", true, NOT_SUPPORTED,
                "Field 'c.DUP' in table 'schema.table' is ambiguous: multiple fields match ignoring case");
    }

    @Test
    public void testCaseCollisionElsewhereDoesNotAffectLookup()
    {
        Schema schema = schema();
        // Schema.caseInsensitiveFindField fails for every name once the schema has a case collision
        assertResolves(schema, "s.a", "s.a");
        assertResolves(schema, "S.B.D", "s.b.d");
        assertResolves(schema, "items.element.name", "items.element.name");
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
        // the same names inside the struct that is the element of a list or the value of a map
        assertResolves(schema, "items.element.value", "items.element.value");
        assertResolves(schema, "m.value.key", "m.value.key");
    }

    @Test
    public void testStructListAndMapTraversal()
    {
        Schema schema = schema();
        assertResolves(schema, "items.element.name", "items.element.name");
        assertResolves(schema, "ITEMS.ELEMENT.NAME", "items.element.name");
        assertResolves(schema, "m.value.b", "m.value.b");
        assertResolves(schema, "M.VALUE.B", "m.value.b");
        // several nested collections
        assertResolves(schema, "ll.element.element.z", "ll.element.element.z");
        assertResolves(schema, "LL.ELEMENT.ELEMENT.Y", "ll.element.element.y");
    }

    @Test
    public void testMapKeyAndCollectionContainersAreRejected()
    {
        Schema schema = schema();
        assertFails(schema, "km.key.k1", false, NOT_SUPPORTED,
                "Cannot drop or traverse the key of map 'km' in table 'schema.table'");
        assertFails(schema, "km.KEY.k1", false, NOT_SUPPORTED,
                "Cannot drop or traverse the key of map 'km' in table 'schema.table'");
        assertFails(schema, "km.key", false, NOT_SUPPORTED,
                "Cannot drop or traverse the key of map 'km' in table 'schema.table'");
        assertFails(schema, "m.key", false, NOT_SUPPORTED,
                "Cannot drop or traverse the key of map 'm' in table 'schema.table'");
        // IF EXISTS does not apply to unsupported operations
        assertFails(schema, "km.key.k1", true, NOT_SUPPORTED,
                "Cannot drop or traverse the key of map 'km' in table 'schema.table'");

        assertFails(schema, "items.element", false, NOT_SUPPORTED,
                "Cannot drop the element of list 'items' in table 'schema.table'");
        assertFails(schema, "ll.element.element", false, NOT_SUPPORTED,
                "Cannot drop the element of list 'll.element' in table 'schema.table'");
        assertFails(schema, "m.value", false, NOT_SUPPORTED,
                "Cannot drop the value of map 'm' in table 'schema.table'");
    }

    @Test
    public void testInvalidCollectionStep()
    {
        Schema schema = schema();
        // a collection can only be traversed through element/value, so this is not a missing parent
        assertFails(schema, "items.wrong.leaf", false, NOT_SUPPORTED,
                "Parent field 'items' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "items.wrong.leaf", true, NOT_SUPPORTED,
                "Parent field 'items' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "items.name", false, NOT_SUPPORTED,
                "Parent field 'items' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "m.other.x", false, NOT_SUPPORTED,
                "Parent field 'm' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "ll.element.wrong.z", false, NOT_SUPPORTED,
                "Parent field 'll.element' in table 'schema.table' is not a struct/ROW type");
    }

    @Test
    public void testMissingParentAndMissingLeaf()
    {
        Schema schema = schema();

        // missing leaf
        assertFails(schema, "s.missing", false, COLUMN_NOT_FOUND,
                "Field 's.missing' does not exist in table 'schema.table'");
        assertFails(schema, "items.element.missing", false, COLUMN_NOT_FOUND,
                "Field 'items.element.missing' does not exist in table 'schema.table'");
        assertFails(schema, "nope", false, COLUMN_NOT_FOUND,
                "Field 'nope' does not exist in table 'schema.table'");
        assertEmpty(schema, "s.missing", true);
        assertEmpty(schema, "items.element.missing", true);
        assertEmpty(schema, "nope", true);

        // missing parent
        assertFails(schema, "s.missing.leaf", false, COLUMN_NOT_FOUND,
                "Cannot find parent field 's.missing' in table 'schema.table'");
        assertFails(schema, "nope.leaf", false, COLUMN_NOT_FOUND,
                "Cannot find parent field 'nope' in table 'schema.table'");
        assertFails(schema, "items.element.missing.leaf", false, COLUMN_NOT_FOUND,
                "Cannot find parent field 'items.element.missing' in table 'schema.table'");
        // the first segment that does not exist is reported, even when more segments follow it
        assertFails(schema, "nope.a.b", false, COLUMN_NOT_FOUND,
                "Cannot find parent field 'nope' in table 'schema.table'");
        assertFails(schema, "s.missing.a.b", false, COLUMN_NOT_FOUND,
                "Cannot find parent field 's.missing' in table 'schema.table'");
        assertEmpty(schema, "nope.a.b", true);
        assertEmpty(schema, "s.missing.leaf", true);
        assertEmpty(schema, "nope.leaf", true);
        assertEmpty(schema, "items.element.missing.leaf", true);
    }

    @Test
    public void testPrimitiveParent()
    {
        Schema schema = schema();
        // a primitive parent is unsupported, with or without IF EXISTS
        assertFails(schema, "s.a.leaf", false, NOT_SUPPORTED,
                "Parent field 's.a' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "s.a.leaf", true, NOT_SUPPORTED,
                "Parent field 's.a' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "plain.leaf", false, NOT_SUPPORTED,
                "Parent field 'plain' in table 'schema.table' is not a struct/ROW type");
        assertFails(schema, "items.element.name.leaf", true, NOT_SUPPORTED,
                "Parent field 'items.element.name' in table 'schema.table' is not a struct/ROW type");
    }

    @Test
    public void testLastFieldGuard()
    {
        Schema schema = schema();
        assertFails(schema, "s.solo.x", false, NOT_SUPPORTED,
                "Cannot drop the only field 'x' in row type: s.solo");
        // the canonical parent path is reported, not the spelling of the request
        assertFails(schema, "S.SOLO.X", false, NOT_SUPPORTED,
                "Cannot drop the only field 'X' in row type: s.solo");
        assertFails(schema, "S.SOLO.X", true, NOT_SUPPORTED,
                "Cannot drop the only field 'X' in row type: s.solo");

        // dropping the struct that contains the only field is fine, as is dropping one of several fields
        assertResolves(schema, "s.solo", "s.solo");
        assertResolves(schema, "s.b.c", "s.b.c");
    }

    private static void assertResolves(Schema schema, String path, String expectedCanonicalPath)
    {
        Optional<NestedField> field = resolveFieldToDrop(schema, parse(path), false, TABLE);
        assertEquals(field.map(value -> schema.findColumnName(value.fieldId())), Optional.of(expectedCanonicalPath));
        // the same request resolves to the same field when IF EXISTS is given
        assertEquals(resolveFieldToDrop(schema, parse(path), true, TABLE), field);
    }

    private static void assertEmpty(Schema schema, String path, boolean ignoreNonExistent)
    {
        assertFalse(resolveFieldToDrop(schema, parse(path), ignoreNonExistent, TABLE).isPresent());
    }

    private static void assertFails(Schema schema, String path, boolean ignoreNonExistent, ErrorCodeSupplier errorCode, String message)
    {
        PrestoException exception = expectThrows(PrestoException.class, () -> resolveFieldToDrop(schema, parse(path), ignoreNonExistent, TABLE));
        assertEquals(exception.getErrorCode(), errorCode.toErrorCode());
        assertEquals(exception.getMessage(), message);
    }

    private static List<String> parse(String path)
    {
        return ImmutableList.copyOf(path.split("\\."));
    }

    private static NestedField field(Ids ids, String name, Type type)
    {
        return NestedField.optional(ids.next(), name, type);
    }

    private static class Ids
    {
        private int next = 1;

        int next()
        {
            return next++;
        }
    }
}
