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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestLocalFileSplit
{
    private static final LocalFileConnectorId CONNECTOR_ID = new LocalFileConnectorId("connectorId");
    private HostAddress address;
    private LocalFileSplit split;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        address = HostAddress.fromParts("localhost", 1234);
        split = new LocalFileSplit(CONNECTOR_ID, address, Resources.getResource("example-data/http-request.log").toURI());
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<LocalFileSplit> codec = jsonCodec(LocalFileSplit.class);
        String json = codec.toJson(split);
        LocalFileSplit copy = codec.fromJson(json);
        assertEquals(copy.getConnectorId(), split.getConnectorId());
        assertEquals(copy.getAddress(), split.getAddress());
        assertEquals(copy.getUri(), split.getUri());

        assertEquals(copy.getAddresses(), ImmutableList.of(address));
        assertEquals(copy.isRemotelyAccessible(), false);
    }
}
