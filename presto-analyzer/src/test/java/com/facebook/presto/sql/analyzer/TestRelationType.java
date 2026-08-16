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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestRelationType
{
    private static final Optional<com.facebook.presto.sql.tree.NodeLocation> NO_LOCATION = Optional.empty();

    @Test
    public void testResolveFieldsUnqualified()
    {
        Field orderIdField = Field.newUnqualified(NO_LOCATION, "order_id", BIGINT);
        Field customerIdField = Field.newUnqualified(NO_LOCATION, "customer_id", BIGINT);
        Field totalField = Field.newUnqualified(NO_LOCATION, "total", BIGINT);

        RelationType relationType = new RelationType(ImmutableList.of(orderIdField, customerIdField, totalField));

        // Resolve by unqualified name
        List<Field> result = relationType.resolveFields(QualifiedName.of("order_id"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), orderIdField);

        // Resolve non-existent field
        result = relationType.resolveFields(QualifiedName.of("nonexistent"));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testResolveFieldsCaseInsensitive()
    {
        Field field = Field.newUnqualified(NO_LOCATION, "Order_ID", BIGINT);
        RelationType relationType = new RelationType(ImmutableList.of(field));

        // Lowercase query
        List<Field> result = relationType.resolveFields(QualifiedName.of("order_id"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), field);

        // Uppercase query
        result = relationType.resolveFields(QualifiedName.of("ORDER_ID"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), field);

        // Mixed case query
        result = relationType.resolveFields(QualifiedName.of("oRdEr_Id"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), field);
    }

    @Test
    public void testResolveFieldsDuplicateNamesDifferingByCase()
    {
        // Two fields with names that differ only by case — both should be
        // resolved since the index is lowercased
        Field upperField = Field.newUnqualified(NO_LOCATION, "Order_ID", BIGINT);
        Field lowerField = Field.newUnqualified(NO_LOCATION, "order_id", BIGINT);

        RelationType relationType = new RelationType(ImmutableList.of(upperField, lowerField));

        // Any casing should resolve to both fields
        List<Field> result = relationType.resolveFields(QualifiedName.of("order_id"));
        assertEquals(result.size(), 2);

        result = relationType.resolveFields(QualifiedName.of("ORDER_ID"));
        assertEquals(result.size(), 2);

        assertTrue(relationType.canResolve(QualifiedName.of("Order_ID")));
    }

    @Test
    public void testResolveFieldsQualified()
    {
        Field field = Field.newQualified(
                NO_LOCATION,
                QualifiedName.of("t"),
                Optional.of("col"),
                BIGINT,
                false,
                Optional.empty(),
                Optional.empty(),
                false);

        RelationType relationType = new RelationType(ImmutableList.of(field));

        // Unqualified — matches (no prefix = always matches)
        List<Field> result = relationType.resolveFields(QualifiedName.of("col"));
        assertEquals(result.size(), 1);

        // Qualified with matching alias
        result = relationType.resolveFields(QualifiedName.of("t", "col"));
        assertEquals(result.size(), 1);

        // Qualified with non-matching alias
        result = relationType.resolveFields(QualifiedName.of("other", "col"));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testResolveFieldsMultiPartAlias()
    {
        // Field with relation alias "x.y"
        Field field = Field.newQualified(
                NO_LOCATION,
                QualifiedName.of("x", "y"),
                Optional.of("a"),
                BIGINT,
                false,
                Optional.empty(),
                Optional.empty(),
                false);

        RelationType relationType = new RelationType(ImmutableList.of(field));

        // "a" — no prefix, matches
        assertTrue(!relationType.resolveFields(QualifiedName.of("a")).isEmpty());

        // "y.a" — prefix "y" is suffix of alias "x.y", matches
        assertTrue(!relationType.resolveFields(QualifiedName.of("y", "a")).isEmpty());

        // "x.y.a" — prefix "x.y" equals alias "x.y", matches
        assertTrue(!relationType.resolveFields(QualifiedName.of("x", "y", "a")).isEmpty());

        // "x.a" — prefix "x" is NOT a suffix of alias "x.y", no match
        assertTrue(relationType.resolveFields(QualifiedName.of("x", "a")).isEmpty());
    }

    @Test
    public void testResolveFieldsAnonymous()
    {
        // Anonymous field (no name) — should never be resolved
        Field anonymousField = Field.newUnqualified(NO_LOCATION, Optional.empty(), BIGINT);
        Field namedField = Field.newUnqualified(NO_LOCATION, "col", BIGINT);

        RelationType relationType = new RelationType(ImmutableList.of(anonymousField, namedField));

        List<Field> result = relationType.resolveFields(QualifiedName.of("col"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), namedField);
    }

    @Test
    public void testResolveFieldsDuplicateNames()
    {
        // Simulate a JOIN that produces duplicate column names
        Field leftCol = Field.newQualified(
                NO_LOCATION,
                QualifiedName.of("left_t"),
                Optional.of("id"),
                BIGINT,
                false,
                Optional.empty(),
                Optional.empty(),
                false);
        Field rightCol = Field.newQualified(
                NO_LOCATION,
                QualifiedName.of("right_t"),
                Optional.of("id"),
                VARCHAR,
                false,
                Optional.empty(),
                Optional.empty(),
                false);

        RelationType relationType = new RelationType(ImmutableList.of(leftCol, rightCol));

        // Unqualified "id" — matches both
        List<Field> result = relationType.resolveFields(QualifiedName.of("id"));
        assertEquals(result.size(), 2);

        // Qualified "left_t.id" — matches only left
        result = relationType.resolveFields(QualifiedName.of("left_t", "id"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), leftCol);

        // Qualified "right_t.id" — matches only right
        result = relationType.resolveFields(QualifiedName.of("right_t", "id"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), rightCol);
    }

    @Test
    public void testResolveFieldsHiddenFields()
    {
        // Hidden fields should still be resolvable (resolveFields uses allFields, not visibleFields)
        Field hiddenField = Field.newQualified(
                NO_LOCATION,
                QualifiedName.of("t"),
                Optional.of("$bucket"),
                BIGINT,
                true,   // hidden
                Optional.empty(),
                Optional.empty(),
                false);
        Field visibleField = Field.newUnqualified(NO_LOCATION, "col", BIGINT);

        RelationType relationType = new RelationType(ImmutableList.of(hiddenField, visibleField));

        // Hidden field is resolvable
        List<Field> result = relationType.resolveFields(QualifiedName.of("$bucket"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), hiddenField);

        // But not in visible fields
        assertFalse(relationType.getVisibleFields().contains(hiddenField));
    }

    @Test
    public void testCanResolve()
    {
        Field field = Field.newUnqualified(NO_LOCATION, "col", BIGINT);
        RelationType relationType = new RelationType(ImmutableList.of(field));

        assertTrue(relationType.canResolve(QualifiedName.of("col")));
        assertTrue(relationType.canResolve(QualifiedName.of("COL")));
        assertFalse(relationType.canResolve(QualifiedName.of("other")));
    }

    @Test
    public void testResolveFieldsWideProjection()
    {
        // Simulate a wide projection with many columns (the optimization target)
        int numColumns = 2225;
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (int i = 0; i < numColumns; i++) {
            fields.add(Field.newUnqualified(NO_LOCATION, "col_" + i, BIGINT));
        }
        RelationType relationType = new RelationType(fields.build());

        // Resolve first, middle, and last columns
        List<Field> result = relationType.resolveFields(QualifiedName.of("col_0"));
        assertEquals(result.size(), 1);

        result = relationType.resolveFields(QualifiedName.of("col_1112"));
        assertEquals(result.size(), 1);

        result = relationType.resolveFields(QualifiedName.of("col_2224"));
        assertEquals(result.size(), 1);

        // Non-existent
        result = relationType.resolveFields(QualifiedName.of("col_9999"));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testResolveFieldsNonAsciiNames()
    {
        Field unicodeField = Field.newUnqualified(NO_LOCATION, "\u00e9v\u00e9nement", BIGINT); // événement
        Field cjkField = Field.newUnqualified(NO_LOCATION, "\u5217\u540d", VARCHAR); // 列名 (Chinese for "column name")
        Field mixedField = Field.newUnqualified(NO_LOCATION, "col_\u00fc\u00df", BIGINT); // col_üß

        RelationType relationType = new RelationType(ImmutableList.of(unicodeField, cjkField, mixedField));

        // Exact match
        List<Field> result = relationType.resolveFields(QualifiedName.of("\u00e9v\u00e9nement"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), unicodeField);

        // CJK characters
        result = relationType.resolveFields(QualifiedName.of("\u5217\u540d"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), cjkField);

        // Mixed ASCII and non-ASCII
        result = relationType.resolveFields(QualifiedName.of("col_\u00fc\u00df"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), mixedField);

        // Case insensitive for Latin characters with diacritics:
        // uppercase of é is É
        result = relationType.resolveFields(QualifiedName.of("\u00c9V\u00c9NEMENT"));
        assertEquals(result.size(), 1);
        assertSame(result.get(0), unicodeField);

        // Non-existent
        result = relationType.resolveFields(QualifiedName.of("\u4e0d\u5b58\u5728"));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testResolveFieldsEmptyRelation()
    {
        RelationType relationType = new RelationType(ImmutableList.of());

        List<Field> result = relationType.resolveFields(QualifiedName.of("anything"));
        assertTrue(result.isEmpty());
        assertFalse(relationType.canResolve(QualifiedName.of("anything")));
    }
}
