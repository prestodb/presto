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
package com.facebook.presto.plugin.opensearch;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.VarcharType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for NestedFieldInfo class.
 */
public class TestNestedFieldInfo
{
    @Test
    public void testBasicConstruction()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "token_usage.total_tokens",
                "token_usage",
                "total_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "total_tokens"));

        assertEquals(info.getFieldPath(), "token_usage.total_tokens");
        assertEquals(info.getParentPath(), "token_usage");
        assertEquals(info.getFieldName(), "total_tokens");
        assertEquals(info.getPrestoType(), BigintType.BIGINT);
        assertEquals(info.getOpenSearchType(), "long");
        assertEquals(info.getNestingLevel(), 2);
        assertTrue(info.isLeafField());
        assertEquals(info.getFieldPathList(), Arrays.asList("token_usage", "total_tokens"));
    }

    @Test
    public void testRootLevelField()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "id",
                "",
                "id",
                VarcharType.VARCHAR,
                "keyword",
                0,
                true,
                Collections.singletonList("id"));

        assertEquals(info.getFieldPath(), "id");
        assertEquals(info.getParentPath(), "");
        assertEquals(info.getFieldName(), "id");
        assertEquals(info.getNestingLevel(), 0);
        assertFalse(info.isNestedField());
    }

    @Test
    public void testIsNestedField()
    {
        NestedFieldInfo nested = new NestedFieldInfo(
                "user.name",
                "user",
                "name",
                VarcharType.VARCHAR,
                "text",
                1,
                true,
                Arrays.asList("user", "name"));

        assertTrue(nested.isNestedField());

        NestedFieldInfo root = new NestedFieldInfo(
                "name",
                "",
                "name",
                VarcharType.VARCHAR,
                "text",
                0,
                true,
                Collections.singletonList("name"));

        assertFalse(root.isNestedField());
    }

    @Test
    public void testJsonPath()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "token_usage.total_tokens",
                "token_usage",
                "total_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "total_tokens"));

        assertEquals(info.getJsonPath(), "$.token_usage.total_tokens");
    }

    @Test
    public void testDeeplyNestedField()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "a.b.c.d.e",
                "a.b.c.d",
                "e",
                VarcharType.VARCHAR,
                "text",
                5,
                true,
                Arrays.asList("a", "b", "c", "d", "e"));

        assertEquals(info.getNestingLevel(), 5);
        assertTrue(info.isNestedField());
        assertEquals(info.getFieldPathList().size(), 5);
    }

    @Test
    public void testNonLeafField()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "user",
                "",
                "user",
                VarcharType.VARCHAR,
                "object",
                1,
                false,
                Collections.singletonList("user"));

        assertFalse(info.isLeafField());
    }

    @Test
    public void testEquality()
    {
        NestedFieldInfo info1 = new NestedFieldInfo(
                "token_usage.total_tokens",
                "token_usage",
                "total_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "total_tokens"));

        NestedFieldInfo info2 = new NestedFieldInfo(
                "token_usage.total_tokens",
                "token_usage",
                "total_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "total_tokens"));

        assertEquals(info1, info2);
        assertEquals(info1.hashCode(), info2.hashCode());
    }

    @Test
    public void testInequality()
    {
        NestedFieldInfo info1 = new NestedFieldInfo(
                "token_usage.total_tokens",
                "token_usage",
                "total_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "total_tokens"));

        NestedFieldInfo info2 = new NestedFieldInfo(
                "token_usage.prompt_tokens",
                "token_usage",
                "prompt_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "prompt_tokens"));

        assertNotEquals(info1, info2);
    }

    @Test
    public void testDifferentTypes()
    {
        NestedFieldInfo bigintField = new NestedFieldInfo(
                "count",
                "",
                "count",
                BigintType.BIGINT,
                "long",
                0,
                true,
                Collections.singletonList("count"));

        NestedFieldInfo doubleField = new NestedFieldInfo(
                "price",
                "",
                "price",
                DoubleType.DOUBLE,
                "double",
                0,
                true,
                Collections.singletonList("price"));

        NestedFieldInfo boolField = new NestedFieldInfo(
                "active",
                "",
                "active",
                BooleanType.BOOLEAN,
                "boolean",
                0,
                true,
                Collections.singletonList("active"));

        assertEquals(bigintField.getPrestoType(), BigintType.BIGINT);
        assertEquals(doubleField.getPrestoType(), DoubleType.DOUBLE);
        assertEquals(boolField.getPrestoType(), BooleanType.BOOLEAN);
    }

    @Test
    public void testToString()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "token_usage.total_tokens",
                "token_usage",
                "total_tokens",
                BigintType.BIGINT,
                "long",
                2,
                true,
                Arrays.asList("token_usage", "total_tokens"));

        String str = info.toString();
        assertTrue(str.contains("token_usage.total_tokens"));
        assertTrue(str.contains("bigint"));
        assertTrue(str.contains("long"));
    }

    @Test
    public void testNullParentPath()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "field",
                null,
                "field",
                VarcharType.VARCHAR,
                "text",
                0,
                true,
                Collections.singletonList("field"));

        assertEquals(info.getParentPath(), "");
    }

    @Test
    public void testEmptyFieldPathList()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "field",
                "",
                "field",
                VarcharType.VARCHAR,
                "text",
                0,
                true,
                null);

        assertEquals(info.getFieldPathList(), Collections.emptyList());
    }

    @Test
    public void testFieldPathListImmutable()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "a.b",
                "a",
                "b",
                VarcharType.VARCHAR,
                "text",
                1,
                true,
                Arrays.asList("a", "b"));

        // Verify the list is unmodifiable
        try {
            info.getFieldPathList().add("c");
            throw new AssertionError("Expected UnsupportedOperationException");
        }
        catch (UnsupportedOperationException e) {
            // Expected
        }
    }

    @Test
    public void testMaxNestingDepth()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "a.b.c.d.e.f",
                "a.b.c.d.e",
                "f",
                VarcharType.VARCHAR,
                "text",
                6,
                true,
                Arrays.asList("a", "b", "c", "d", "e", "f"));

        assertEquals(info.getNestingLevel(), 6);
        assertTrue(info.isNestedField());
    }

    @Test
    public void testFieldNameExtraction()
    {
        NestedFieldInfo info = new NestedFieldInfo(
                "user.profile.email",
                "user.profile",
                "email",
                VarcharType.VARCHAR,
                "keyword",
                2,
                true,
                Arrays.asList("user", "profile", "email"));

        assertEquals(info.getFieldName(), "email");
        assertEquals(info.getParentPath(), "user.profile");
    }
}
