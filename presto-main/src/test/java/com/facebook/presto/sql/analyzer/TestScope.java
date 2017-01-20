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

import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestScope
{
    @Test
    public void test()
    {
        Scope root = Scope.builder().build();

        Field outerColumn1 = Field.newQualified(QualifiedName.of("outer", "column1"), Optional.of("c1"), BIGINT, false, Optional.empty(), false);
        Field outerColumn2 = Field.newQualified(QualifiedName.of("outer", "column2"), Optional.of("c2"), BIGINT, false, Optional.empty(), false);
        Scope outer = Scope.builder().withParent(root).withRelationType(new RelationType(outerColumn1, outerColumn2)).build();

        Field innerColumn2 = Field.newQualified(QualifiedName.of("inner", "column2"), Optional.of("c2"), BIGINT, false, Optional.empty(), false);
        Field innerColumn3 = Field.newQualified(QualifiedName.of("inner", "column3"), Optional.of("c3"), BIGINT, false, Optional.empty(), false);
        Scope inner = Scope.builder().withParent(outer).withRelationType(new RelationType(innerColumn2, innerColumn3)).build();

        Expression c1 = name("c1");
        Expression c2 = name("c2");
        Expression c3 = name("c3");
        Expression c4 = name("c4");

        assertFalse(root.tryResolveField(c1).isPresent());

        assertTrue(outer.tryResolveField(c1).isPresent());
        assertEquals(outer.tryResolveField(c1).get().getField(), outerColumn1);
        assertEquals(outer.tryResolveField(c1).get().isLocal(), true);
        assertTrue(outer.tryResolveField(c2).isPresent());
        assertEquals(outer.tryResolveField(c2).get().getField(), outerColumn2);
        assertEquals(outer.tryResolveField(c2).get().isLocal(), true);
        assertFalse(outer.tryResolveField(c3).isPresent());
        assertFalse(outer.tryResolveField(c4).isPresent());

        assertTrue(inner.tryResolveField(c1).isPresent());
        assertEquals(inner.tryResolveField(c1).get().getField(), outerColumn1);
        assertEquals(inner.tryResolveField(c1).get().isLocal(), false);
        assertTrue(inner.tryResolveField(c2).isPresent());
        assertEquals(inner.tryResolveField(c2).get().getField(), innerColumn2);
        assertEquals(inner.tryResolveField(c2).get().isLocal(), true);
        assertTrue(inner.tryResolveField(c2).isPresent());
        assertEquals(inner.tryResolveField(c3).get().getField(), innerColumn3);
        assertEquals(inner.tryResolveField(c3).get().isLocal(), true);
        assertFalse(inner.tryResolveField(c4).isPresent());
    }

    private static Expression name(String first, String... parts)
    {
        return DereferenceExpression.from(QualifiedName.of(first, parts));
    }
}
