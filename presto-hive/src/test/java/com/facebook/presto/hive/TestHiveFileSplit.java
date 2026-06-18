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
package com.facebook.presto.hive;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

@Test
public class TestHiveFileSplit
{
    @Test
    public void testGetters()
    {
        HiveFileSplit split = new HiveFileSplit("path", 0, 200, 3, 400, Optional.of(new byte[21]), Collections.emptyMap(), 0);
        assertEquals(split.getPath(), "path");
        assertEquals(split.getLength(), 200L);
        assertEquals(split.getStart(), 0L);
        assertEquals(split.getFileSize(), 3L);
        assertEquals(split.getFileModifiedTime(), 400L);
        assertEquals(split.getExtraFileInfo().get().length, 21);
        assertEquals(split.getCustomSplitInfo().size(), 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "start must be non-negative")
    public void testNegativeStart()
    {
        new HiveFileSplit("path", -1, 200, 3, 400, Optional.of(new byte[21]), Collections.emptyMap(), 0);
    }
}
