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
package com.facebook.presto.server.testing;

import com.google.common.base.Splitter;
import io.airlift.airline.Option;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class TestingPrestoServerLauncherOptions
{
    private static final Splitter CATALOG_OPTION_SPLITTER = Splitter.on(':').trimResults();

    public static class Catalog
    {
        private final String catalogName;
        private final String connectorName;

        public Catalog(String catalogName, String connectorName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.connectorName = requireNonNull(connectorName, "connectorName is null");
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public String getConnectorName()
        {
            return connectorName;
        }
    }

    @Option(name = "--catalog", title = "catalog", description = "Catalog:Connector mapping (can be repeated)")
    private List<String> catalogOptions = new ArrayList<>();

    @Option(name = "--plugin", title = "plugin", description = "Fully qualified class name of plugin to be registered (can be repeated)")
    private List<String> pluginClassNames = new ArrayList<>();

    public List<Catalog> getCatalogs()
    {
        return catalogOptions.stream().map(catalogOption -> {
            List<String> parts = copyOf(CATALOG_OPTION_SPLITTER.split(catalogOption));
            checkState(parts.size() == 2, "bad format of catalog definition '%s'; should be catalog_name:connector_name", catalogOption);
            return new Catalog(parts.get(0), parts.get(1));
        }).collect(toList());
    }

    public List<String> getPluginClassNames()
    {
        return pluginClassNames;
    }

    public void validate()
    {
        checkState(!pluginClassNames.isEmpty(), "some plugins must be defined");
        checkState(!catalogOptions.isEmpty(), "some catalogs must be defined");
        getCatalogs();
    }
}
