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
import com.facebook.presto.sql.tree.QualifiedNameReference;
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
        Scope outer = Scope.builder().withParent(root).withRelationType(new RelationType(outerColumn1, outerColumn2)).markQueryBoundary().build();

        Field inner1Column2 = Field.newQualified(QualifiedName.of("inner1", "column2"), Optional.of("c2"), BIGINT, false, Optional.empty(), false);
        Field inner1Column3 = Field.newQualified(QualifiedName.of("inner1", "column3"), Optional.of("c3"), BIGINT, false, Optional.empty(), false);
        Scope inner1 = Scope.builder().withParent(outer).withRelationType(new RelationType(inner1Column2, inner1Column3)).markQueryBoundary().build();

        Field inner2Column4 = Field.newQualified(QualifiedName.of("inner2", "column4"), Optional.of("c4"), BIGINT, false, Optional.empty(), false);
        Scope inner2 = Scope.builder().withParent(inner1).withRelationType(new RelationType(inner2Column4)).build();

        QualifiedNameReference c1 = name("c1");
        QualifiedNameReference c2 = name("c2");
        QualifiedNameReference c3 = name("c3");
        QualifiedNameReference c4 = name("c4");

        assertFalse(root.tryResolveField(c1).isPresent());

        assertTrue(outer.tryResolveField(c1).isPresent());
        assertEquals(outer.tryResolveField(c1).get().getField(), outerColumn1);
        assertEquals(outer.tryResolveField(c1).get().isLocal(), true);
        assertTrue(outer.tryResolveField(c2).isPresent());
        assertEquals(outer.tryResolveField(c2).get().getField(), outerColumn2);
        assertEquals(outer.tryResolveField(c2).get().isLocal(), true);
        assertFalse(outer.tryResolveField(c3).isPresent());
        assertFalse(outer.tryResolveField(c4).isPresent());

        assertTrue(inner1.tryResolveField(c1).isPresent());
        assertEquals(inner1.tryResolveField(c1).get().getField(), outerColumn1);
        assertEquals(inner1.tryResolveField(c1).get().isLocal(), false);
        assertTrue(inner1.tryResolveField(c2).isPresent());
        assertEquals(inner1.tryResolveField(c2).get().getField(), inner1Column2);
        assertEquals(inner1.tryResolveField(c2).get().isLocal(), true);
        assertTrue(inner1.tryResolveField(c2).isPresent());
        assertEquals(inner1.tryResolveField(c3).get().getField(), inner1Column3);
        assertEquals(inner1.tryResolveField(c3).get().isLocal(), true);
        assertFalse(inner1.tryResolveField(c4).isPresent());

        assertTrue(inner2.tryResolveField(c1).isPresent());
        assertEquals(inner2.tryResolveField(c1).get().getField(), outerColumn1);
        assertEquals(inner2.tryResolveField(c1).get().isLocal(), false);
        assertTrue(inner2.tryResolveField(c2).isPresent());
        assertEquals(inner2.tryResolveField(c2).get().getField(), outerColumn2);
        assertEquals(inner2.tryResolveField(c2).get().isLocal(), false);
        assertFalse(inner2.tryResolveField(c3).isPresent());
        assertTrue(inner2.tryResolveField(c4).isPresent());
        assertEquals(inner2.tryResolveField(c4).get().getField(), inner2Column4);
        assertEquals(inner2.tryResolveField(c4).get().isLocal(), true);
    }

    private QualifiedNameReference name(String first, String... parts)
    {
        return new QualifiedNameReference(QualifiedName.of(first, parts));
    }
}
