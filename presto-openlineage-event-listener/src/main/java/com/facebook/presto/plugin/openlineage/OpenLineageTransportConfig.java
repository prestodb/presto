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
package com.facebook.presto.plugin.openlineage;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OpenLineageTransportConfig
{
    private OpenLineageTransport transport = OpenLineageTransport.CONSOLE;

    public OpenLineageTransportConfig()
    {
    }

    public OpenLineageTransportConfig(Map<String, String> config)
    {
        requireNonNull(config, "config is null");
        String transportType = config.get("openlineage-event-listener.transport.type");
        if (transportType != null) {
            this.transport = OpenLineageTransport.valueOf(transportType.toUpperCase());
        }
    }

    public OpenLineageTransport getTransport()
    {
        return transport;
    }

    public OpenLineageTransportConfig setTransport(OpenLineageTransport transport)
    {
        this.transport = transport;
        return this;
    }
}
