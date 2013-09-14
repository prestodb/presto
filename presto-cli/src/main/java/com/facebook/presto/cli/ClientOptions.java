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
import io.airlift.command.Option;

import java.net.URI;

public class ClientOptions
{
    @Option(name = "--server", title = "server")
    public URI server = URI.create("http://localhost:8080");

    @Option(name = "--user", title = "user")
    public String user = System.getProperty("user.name");

    @Option(name = "--catalog", title = "catalog")
    public String catalog = "default";

    @Option(name = "--schema", title = "schema")
    public String schema = "default";

    @Option(name = {"-f", "--file"}, title = "file")
    public String file;

    @Option(name = "--debug", title = "debug")
    public boolean debug;

    @Option(name = "--execute", title = "execute")
    public String execute;

    @Option(name = "--output-format", title = "output-format")
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
        return new ClientSession(server, user, "presto-cli", catalog, schema, debug);
    }
}
