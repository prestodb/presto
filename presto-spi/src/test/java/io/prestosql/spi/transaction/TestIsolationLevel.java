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
package io.prestosql.spi.transaction;

import org.testng.annotations.Test;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.REPEATABLE_READ;
import static io.prestosql.spi.transaction.IsolationLevel.SERIALIZABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIsolationLevel
{
    @Test
    public void testMeetsRequirementOf()
    {
        assertTrue(READ_UNCOMMITTED.meetsRequirementOf(READ_UNCOMMITTED));
        assertFalse(READ_UNCOMMITTED.meetsRequirementOf(READ_COMMITTED));
        assertFalse(READ_UNCOMMITTED.meetsRequirementOf(REPEATABLE_READ));
        assertFalse(READ_UNCOMMITTED.meetsRequirementOf(SERIALIZABLE));

        assertTrue(READ_COMMITTED.meetsRequirementOf(READ_UNCOMMITTED));
        assertTrue(READ_COMMITTED.meetsRequirementOf(READ_COMMITTED));
        assertFalse(READ_COMMITTED.meetsRequirementOf(REPEATABLE_READ));
        assertFalse(READ_COMMITTED.meetsRequirementOf(SERIALIZABLE));

        assertTrue(REPEATABLE_READ.meetsRequirementOf(READ_UNCOMMITTED));
        assertTrue(REPEATABLE_READ.meetsRequirementOf(READ_COMMITTED));
        assertTrue(REPEATABLE_READ.meetsRequirementOf(REPEATABLE_READ));
        assertFalse(REPEATABLE_READ.meetsRequirementOf(SERIALIZABLE));

        assertTrue(SERIALIZABLE.meetsRequirementOf(READ_UNCOMMITTED));
        assertTrue(SERIALIZABLE.meetsRequirementOf(READ_COMMITTED));
        assertTrue(SERIALIZABLE.meetsRequirementOf(REPEATABLE_READ));
        assertTrue(SERIALIZABLE.meetsRequirementOf(SERIALIZABLE));
    }

    @Test
    public void testToString()
    {
        assertEquals(READ_UNCOMMITTED.toString(), "READ UNCOMMITTED");
        assertEquals(READ_COMMITTED.toString(), "READ COMMITTED");
        assertEquals(REPEATABLE_READ.toString(), "REPEATABLE READ");
        assertEquals(SERIALIZABLE.toString(), "SERIALIZABLE");
    }
}
