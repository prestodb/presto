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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.command.Option;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Locale.ENGLISH;

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

    @Option(name = "--session", title = "session", description = "Session property (property can be used multiple times; format is key=value)")
    public final List<ClientSessionProperty> sessionProperties = new ArrayList<>();

    @Option(name = "--socks-proxy", title = "socks-proxy", description = "SOCKS proxy to use for server connections")
    public HostAndPort socksProxy;

    public enum OutputFormat
    {
        ALIGNED,
        VERTICAL,
        CSV,
        TSV,
        CSV_HEADER,
        TSV_HEADER,
        NULL
    }

    public ClientSession toClientSession()
    {
        return new ClientSession(
                parseServer(server),
                user,
                source,
                catalog,
                schema,
                TimeZone.getDefault().getID(),
                Locale.getDefault(),
                toProperties(sessionProperties),
                debug);
    }

    public static URI parseServer(String server)
    {
        server = server.toLowerCase(ENGLISH);
        if (server.startsWith("http://") || server.startsWith("https://")) {
            return URI.create(server);
        }

        HostAndPort host = HostAndPort.fromString(server);
        try {
            return new URI("http", null, host.getHostText(), host.getPortOrDefault(80), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Map<String, String> toProperties(List<ClientSessionProperty> sessionProperties)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientSessionProperty sessionProperty : sessionProperties) {
            String name = sessionProperty.getName();
            if (sessionProperty.getCatalog().isPresent()) {
                name = sessionProperty.getCatalog().get() + "." + name;
            }
            builder.put(name, sessionProperty.getValue());
        }
        return builder.build();
    }

    public static final class ClientSessionProperty
    {
        private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
        private static final Splitter NAME_SPLITTER = Splitter.on('.');
        private final Optional<String> catalog;
        private final String name;
        private final String value;

        public ClientSessionProperty(String property)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(property);
            checkArgument(nameValue.size() == 2, "Session property: %s", property);

            List<String> nameParts = NAME_SPLITTER.splitToList(nameValue.get(0));
            checkArgument(nameParts.size() == 1 || nameParts.size() == 2, "Invalid session property: %s", property);
            if (nameParts.size() == 1) {
                catalog = Optional.empty();
                name = nameParts.get(0);
            }
            else {
                catalog = Optional.of(nameParts.get(0));
                name = nameParts.get(1);
            }
            value = nameValue.get(1);

            verifyProperty(catalog, name, value);
        }

        public ClientSessionProperty(Optional<String> catalog, String name, String value)
        {
            this.catalog = checkNotNull(catalog, "catalog is null");
            this.name = checkNotNull(name, "name is null");
            this.value = checkNotNull(value, "value is null");

            verifyProperty(catalog, name, value);
        }

        private static void verifyProperty(Optional<String> catalog, String name, String value)
        {
            checkArgument(!catalog.isPresent() || !catalog.get().isEmpty(), "Invalid session property: %s.%s:%s", catalog, name, value);
            checkArgument(!name.isEmpty(), "Session property name is empty");

            CharsetEncoder charsetEncoder = US_ASCII.newEncoder();
            checkArgument(catalog.orElse("").indexOf('=') < 0, "Session property catalog must not contain '=': %s", name);
            checkArgument(charsetEncoder.canEncode(catalog.orElse("")), "Session property catalog is not US_ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Session property name must not contain '=': %s", name);
            checkArgument(charsetEncoder.canEncode(name), "Session property name is not US_ASCII: %s", name);
            checkArgument(charsetEncoder.canEncode(value), "Session property value is not US_ASCII: %s", value);
        }

        public Optional<String> getCatalog()
        {
            return catalog;
        }

        public String getName()
        {
            return name;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return (catalog.isPresent() ? catalog.get() + '.' : "") + name + '=' + value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalog, name, value);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ClientSessionProperty other = (ClientSessionProperty) obj;
            return Objects.equals(this.catalog, other.catalog) &&
                    Objects.equals(this.name, other.name) &&
                    Objects.equals(this.value, other.value);
        }
    }
}
