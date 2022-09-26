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

package com.facebook.presto.pinot;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPinotSessionProperties
{
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidNumSegmentSplits()
    {
        new PinotConfig().setNumSegmentsPerSplit(-3);
    }

    @Test
    public void testDistinctCountFunctionNameParsedProperly()
    {
        PinotConfig pinotConfig = new PinotConfig().setOverrideDistinctCountFunction("distinctCountBitmap");
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(pinotConfig);
        ConnectorSession session = new TestingConnectorSession(pinotSessionProperties.getSessionProperties());
        assertEquals(PinotSessionProperties.getOverrideDistinctCountFunction(session), "distinctCountBitmap");
    }
}
