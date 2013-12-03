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

import com.facebook.presto.client.ClientSession;
import com.google.common.net.HostAndPort;
import io.airlift.command.Option;

import java.net.URI;
import java.net.URISyntaxException;

public class ClientOptions
{
    @Option(name = "--server", title = "server", description = "Presto server location (default: localhost:8080)")
    public String server = "localhost:8080";

    @Option(name = "--user", title = "user", description = "Username")
    public String user = System.getProperty("user.name");

    @Option(name = "--source", title = "source", description = "Name of source making query")
    public String source = "presto-cli";

    @Option(name = "--catalog", title = "catalog", description = "Default catalog")
    public String catalog = "default";

    @Option(name = "--schema", title = "schema", description = "Default schema")
    public String schema = "default";

    @Option(name = {"-f", "--file"}, title = "file", description = "Execute statements from file and exit")
    public String file;

    @Option(name = "--debug", title = "debug", description = "Enable debug information")
    public boolean debug;

    @Option(name = "--execute", title = "execute", description = "Execute specified statements and exit")
    public String execute;

    @Option(name = "--output-format", title = "output-format", description = "Output format for batch mode (default: CSV)")
    public OutputFormat outputFormat = OutputFormat.CSV;

    public enum OutputFormat
    {
        ALIGNED,
        VERTICAL,
        CSV,
        TSV,
        CSV_HEADER,
        TSV_HEADER
    }

    public ClientSession toClientSession()
    {
        return new ClientSession(parseServer(server), user, source, catalog, schema, debug);
    }

    private static URI parseServer(String s)
    {
        s = s.toLowerCase();
        if (s.startsWith("http://") || s.startsWith("https://")) {
            return URI.create(s);
        }

        HostAndPort host = HostAndPort.fromString(s);
        try {
            return new URI("http", null, host.getHostText(), host.getPortOrDefault(80), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
