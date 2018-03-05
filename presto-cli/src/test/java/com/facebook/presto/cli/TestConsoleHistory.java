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

import java.io.File;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TestConsoleHistory
{

    @Test
    public void testNonExistingHomeFolder() throws Exception
    {
        final File historyFile = new File("/?", ".history");
        assertFalse(historyFile.canRead(), "shouldn't read invalid location");
        assertFalse(historyFile.canWrite(), "shouldn't write invalid location");
        MemoryHistory result = Console.getHistory(historyFile);
        assertNotNull(result, "result is null");
        result.add("foo foo");
        assertFalse(result instanceof PersistentHistory, "must not be PersistentHistory");
    }
}
