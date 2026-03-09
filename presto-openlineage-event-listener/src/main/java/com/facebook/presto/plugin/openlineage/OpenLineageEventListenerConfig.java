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

import com.facebook.presto.common.resourceGroups.QueryType;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class OpenLineageEventListenerConfig
{
    private URI prestoURI;
    private Set<OpenLineagePrestoFacet> disabledFacets = ImmutableSet.of();
    private Optional<String> namespace = Optional.empty();
    private String jobNameFormat = "$QUERY_ID";

    private Set<QueryType> includeQueryTypes = ImmutableSet.<QueryType>builder()
            .add(QueryType.DELETE)
            .add(QueryType.INSERT)
            .add(QueryType.MERGE)
            .add(QueryType.UPDATE)
            .add(QueryType.DATA_DEFINITION)
            .build();

    public OpenLineageEventListenerConfig()
    {
    }

    public OpenLineageEventListenerConfig(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        String uriStr = config.get("openlineage-event-listener.presto.uri");
        if (uriStr != null) {
            try {
                this.prestoURI = new URI(uriStr);
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid Presto URI: " + uriStr, e);
            }
        }

        String queryTypesStr = config.get("openlineage-event-listener.presto.include-query-types");
        if (queryTypesStr != null && !queryTypesStr.isEmpty()) {
            this.includeQueryTypes = Arrays.stream(queryTypesStr.split(","))
                    .map(String::trim)
                    .map(QueryType::valueOf)
                    .collect(Collectors.toSet());
        }

        String disabledFacetsStr = config.get("openlineage-event-listener.disabled-facets");
        if (disabledFacetsStr != null && !disabledFacetsStr.isEmpty()) {
            this.disabledFacets = Arrays.stream(disabledFacetsStr.split(","))
                    .map(String::trim)
                    .map(s -> OpenLineagePrestoFacet.valueOf(s.toUpperCase()))
                    .collect(Collectors.toSet());
        }

        String namespaceStr = config.get("openlineage-event-listener.namespace");
        this.namespace = Optional.ofNullable(namespaceStr);

        String jobNameFormatStr = config.get("openlineage-event-listener.job.name-format");
        if (jobNameFormatStr != null) {
            this.jobNameFormat = jobNameFormatStr;
        }
    }

    public URI getPrestoURI()
    {
        return prestoURI;
    }

    public OpenLineageEventListenerConfig setPrestoURI(URI prestoURI)
    {
        this.prestoURI = prestoURI;
        return this;
    }

    public Set<QueryType> getIncludeQueryTypes()
    {
        return includeQueryTypes;
    }

    public OpenLineageEventListenerConfig setIncludeQueryTypes(Set<QueryType> includeQueryTypes)
    {
        this.includeQueryTypes = ImmutableSet.copyOf(includeQueryTypes);
        return this;
    }

    public Set<OpenLineagePrestoFacet> getDisabledFacets()
    {
        return disabledFacets;
    }

    public OpenLineageEventListenerConfig setDisabledFacets(Set<OpenLineagePrestoFacet> disabledFacets)
    {
        this.disabledFacets = ImmutableSet.copyOf(disabledFacets);
        return this;
    }

    public Optional<String> getNamespace()
    {
        return namespace;
    }

    public OpenLineageEventListenerConfig setNamespace(String namespace)
    {
        this.namespace = Optional.ofNullable(namespace);
        return this;
    }

    public String getJobNameFormat()
    {
        return jobNameFormat;
    }

    public OpenLineageEventListenerConfig setJobNameFormat(String jobNameFormat)
    {
        this.jobNameFormat = jobNameFormat;
        return this;
    }

    public boolean isJobNameFormatValid()
    {
        return FormatInterpolator.hasValidPlaceholders(jobNameFormat, OpenLineageJobInterpolatedValues.values());
    }
}
