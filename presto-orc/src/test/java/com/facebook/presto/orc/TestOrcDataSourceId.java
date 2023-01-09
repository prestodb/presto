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
package com.facebook.presto.orc;

import com.google.common.base.Throwables;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrcDataSourceId
{
    @Test
    public void testAttachToException()
    {
        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("test id");
        try {
            throw new RuntimeException("Top exception");
        }
        catch (RuntimeException e) {
            orcDataSourceId.attachToException(e);
            e.printStackTrace();

            assertEquals(e.getSuppressed().length, 1);
            assertEquals(e.getSuppressed()[0].getMessage(), "Failed to read ORC file: test id");
            assertEquals(e.getSuppressed()[0].getStackTrace().length, 0);

            String stackTrace = Throwables.getStackTraceAsString(e);
            assertTrue(stackTrace.contains("Top exception"));
            assertTrue(stackTrace.contains("Failed to read ORC file: test id"));
        }
    }
}
