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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestRaptorSessionProperties
{
    private static final ConnectorSession SESSION = new TestingConnectorSession(
            new RaptorSessionProperties(new StorageManagerConfig()).getSessionProperties());

    @Test
    public void testWriterMaxBufferSize()
    {
        DataSize maxBufferSize = RaptorSessionProperties.getWriterMaxBufferSize(SESSION);
        assertEquals(maxBufferSize, new StorageManagerConfig().getMaxBufferSize());
    }
}
