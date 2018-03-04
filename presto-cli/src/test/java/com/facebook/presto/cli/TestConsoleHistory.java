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
package com.facebook.presto.cli;

import jline.console.history.MemoryHistory;
import jline.console.history.PersistentHistory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TestConsoleHistory
{
    private static Object invokeGetHistory() throws Exception
    {
        final Method declaredMethod = Console.class.getDeclaredMethod("getHistory");
        declaredMethod.setAccessible(true);
        return declaredMethod.invoke(null);
    }

    @Test
    public void testNonExistingHomeFolder() throws Exception
    {
        System.setProperty("user.home", "/?");
        final Object result = invokeGetHistory();
        assertNotNull(result, "result is null");
        final MemoryHistory history = (MemoryHistory) result;
        history.add("foo foo");
        assertFalse(history instanceof PersistentHistory, "must not be PersistentHistory");
    }
}
