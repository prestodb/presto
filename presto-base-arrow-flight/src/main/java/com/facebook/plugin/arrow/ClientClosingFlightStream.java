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
package com.facebook.plugin.arrow;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

import static java.util.Objects.requireNonNull;

public class ClientClosingFlightStream
        implements AutoCloseable
{
    private final FlightStream flightStream;
    private final AutoCloseable flightClient;

    public ClientClosingFlightStream(FlightStream flightStream, AutoCloseable flightClient)
    {
        this.flightStream = requireNonNull(flightStream, "flightStream is null");
        this.flightClient = requireNonNull(flightClient, "flightClient is null");
    }

    public VectorSchemaRoot getRoot()
    {
        return flightStream.getRoot();
    }

    public DictionaryProvider getDictionaryProvider()
    {
        return flightStream.getDictionaryProvider();
    }

    public boolean next()
    {
        return flightStream.next();
    }

    @Override
    public void close()
            throws Exception
    {
        try {
            flightStream.close();
        }
        finally {
            flightClient.close();
        }
    }
}
