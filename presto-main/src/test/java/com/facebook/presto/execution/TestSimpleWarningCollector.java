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
package com.facebook.presto.execution;

import com.facebook.presto.client.QueryError;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestSimpleWarningCollector
{
    private static final QueryError WARNING_1 = new QueryError("msg 1", null, 10, "warning 1", "WARNING", null, null);
    private static final QueryError WARNING_2 = new QueryError("msg 2", null, 10, "warning 1", "WARNING", null, null);
    private static final QueryError WARNING_3 = new QueryError("msg 1", null, 20, "warning 2", "WARNING", null, null);
    private static final QueryError WARNING_4 = new QueryError("msg 1", null, 10, "warning 1", "WARNING", null, null);  // Same as WARNING_1, to check equality works.

    @Test
    public void testDedup()
            throws Exception
    {
        WarningCollector warningCollector = new SimpleWarningCollector();
        warningCollector.addWarning(WARNING_1);
        warningCollector.addWarning(WARNING_2);
        warningCollector.addWarning(WARNING_3);
        warningCollector.addWarning(WARNING_4);
        warningCollector.addWarning(WARNING_2);  // Same reference.

        List<QueryError> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 3);

        assertEquals(WARNING_1, warnings.get(0));
        assertEquals(WARNING_2, warnings.get(1));
        assertEquals(WARNING_3, warnings.get(2));
    }

    @Test
    public void testOrder1()
            throws Exception
    {
        WarningCollector warningCollector = new SimpleWarningCollector();
        warningCollector.addWarning(WARNING_1);
        warningCollector.addWarning(WARNING_2);
        warningCollector.addWarning(WARNING_3);

        List<QueryError> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 3);

        assertEquals(WARNING_1, warnings.get(0));
        assertEquals(WARNING_2, warnings.get(1));
        assertEquals(WARNING_3, warnings.get(2));
    }

    @Test
    public void testOrder2()
            throws Exception
    {
        WarningCollector warningCollector = new SimpleWarningCollector();
        warningCollector.addWarning(WARNING_3);
        warningCollector.addWarning(WARNING_2);
        warningCollector.addWarning(WARNING_1);

        List<QueryError> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 3);

        assertEquals(WARNING_3, warnings.get(0));
        assertEquals(WARNING_2, warnings.get(1));
        assertEquals(WARNING_1, warnings.get(2));
    }
}
