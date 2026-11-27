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
package com.facebook.presto.ducklake.catalog;

import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestSnapshotPredicate
{
    @Test
    public void testRendersSqlAndParameters()
    {
        SnapshotPredicate predicate = new SnapshotPredicate("t", 5L);

        assertEquals(predicate.sql(), "? >= t.begin_snapshot AND (? < t.end_snapshot OR t.end_snapshot IS NULL)");
        assertEquals(predicate.parameters(), Arrays.asList(5L, 5L));
    }

    @Test
    public void testRejectsEmptyAlias()
    {
        assertThrows(IllegalArgumentException.class, () -> new SnapshotPredicate("", 5L));
    }
}
