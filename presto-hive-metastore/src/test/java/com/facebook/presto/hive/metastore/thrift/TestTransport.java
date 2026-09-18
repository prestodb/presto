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
package com.facebook.presto.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

public class TestTransport
{
    private static final HostAndPort ADDRESS = HostAndPort.fromParts("localhost", 9083);

    // Simulates Hive's TUGIAssumingTransport returning null from getConfiguration()
    @Test
    public void testGetConfigurationFallsBackToDefaultWhenWrappedTransportReturnsNull()
    {
        TTransport wrapper = new Transport.TTransportWrapper(new NullConfigTransport(), ADDRESS);

        TConfiguration config = wrapper.getConfiguration();

        assertNotNull(config, "TTransportWrapper.getConfiguration() must never return null");
        assertSame(config, TConfiguration.DEFAULT,
                "TTransportWrapper must return TConfiguration.DEFAULT when wrapped transport returns null");
    }

    /**
     * Verify that TTransportWrapper delegates getConfiguration() to the wrapped transport
     * when the wrapped transport returns a non-null value (normal TSocket path).
     */
    @Test
    public void testGetConfigurationDelegatesWhenWrappedTransportReturnsNonNull()
    {
        TConfiguration expected = new TConfiguration();
        TTransport wrapper = new Transport.TTransportWrapper(new FixedConfigTransport(expected), ADDRESS);

        assertSame(wrapper.getConfiguration(), expected,
                "TTransportWrapper must delegate getConfiguration() to the wrapped transport when non-null");
    }

    /**
     * Simulates Hive's TFilterTransport compiled against pre-0.21 libthrift.
     * getConfiguration() returns null because that abstract method did not exist
     * in the old libthrift API when the class was compiled.
     */
    private static class NullConfigTransport
            extends TTransport
    {
        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public int read(byte[] buf, int off, int len)
        {
            return 0;
        }

        @Override
        public void write(byte[] buf, int off, int len) {}

        @Override
        public TConfiguration getConfiguration()
        {
            return null;
        }

        @Override
        public void updateKnownMessageSize(long size) {}

        @Override
        public void checkReadBytesAvailable(long numBytes) {}
    }

    /**
     * Simulates a well-behaved transport (e.g. TSocket) that returns a real TConfiguration.
     */
    private static class FixedConfigTransport
            extends TTransport
    {
        private final TConfiguration configuration;

        FixedConfigTransport(TConfiguration configuration)
        {
            this.configuration = configuration;
        }

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public int read(byte[] buf, int off, int len)
        {
            return 0;
        }

        @Override
        public void write(byte[] buf, int off, int len) {}

        @Override
        public TConfiguration getConfiguration()
        {
            return configuration;
        }

        @Override
        public void updateKnownMessageSize(long size) {}

        @Override
        public void checkReadBytesAvailable(long numBytes) {}
    }
}
