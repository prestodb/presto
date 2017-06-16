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
package com.facebook.presto.benchmark.driver;

import com.facebook.presto.client.ClientSession;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.airline.Option;
import io.airlift.units.Duration;

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
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class BenchmarkDriverOptions
{
    @Option(name = "--server", title = "server", description = "Presto server location (default: localhost:8080)")
    public String server = "localhost:8080";

    @Option(name = "--user", title = "user", description = "Username")
    public String user = System.getProperty("user.name");

    @Option(name = "--catalog", title = "catalog", description = "Default catalog")
    public String catalog;

    @Option(name = "--schema", title = "schema", description = "Default schema")
    public String schema;

    @Option(name = "--suite", title = "suite", description = "Suite to execute")
    public List<String> suites = new ArrayList<>();

    @Option(name = "--suite-config", title = "suite-config", description = "Suites configuration file (default: suite.json)")
    public String suiteConfigFile = "suite.json";

    @Option(name = "--sql", title = "sql", description = "Directory containing sql files (default: sql)")
    public String sqlTemplateDir = "sql";

    @Option(name = "--query", title = "query", description = "Queries to execute")
    public List<String> queries = new ArrayList<>();

    @Option(name = "--debug", title = "debug", description = "Enable debug information (default: false)")
    public boolean debug;

    @Option(name = "--quiet", title = "quiet", description = "Enable quiet mode (less verbose error messages) (default: false)")
    public boolean quiet;

    @Option(name = "--session", title = "session", description = "Session property (property can be used multiple times; format is key=value)")
    public final List<ClientSessionProperty> sessionProperties = new ArrayList<>();

    @Option(name = "--runs", title = "runs", description = "Number of times to run each query (default: 3)")
    public int runs = 3;

    @Option(name = "--warm", title = "warm", description = "Number of times to run each query for a warm-up (default: 1)")
    public int warm = 1;

    @Option(name = "--max-failures", title = "max failures", description = "Max number of consecutive failures before benchmark fails")
    public int maxFailures = 10;

    @Option(name = "--socks", title = "socks", description = "Socks proxy to use")
    public HostAndPort socksProxy;

    @Option(name = "--client-request-timeout", title = "client request timeout", description = "Client request timeout (default: 2m)")
    public Duration clientRequestTimeout = new Duration(2, MINUTES);

    public ClientSession getClientSession()
    {
        return new ClientSession(
                parseServer(server),
                user,
                "presto-benchmark",
                null,
                catalog,
                schema,
                TimeZone.getDefault().getID(),
                Locale.getDefault(),
                toProperties(this.sessionProperties),
                null,
                debug,
                quiet,
                clientRequestTimeout);
    }

    private static URI parseServer(String server)
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

    private static Map<String, String> toProperties(List<ClientSessionProperty> sessionProperties)
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
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.name = requireNonNull(name, "name is null");
            this.value = requireNonNull(value, "value is null");

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
