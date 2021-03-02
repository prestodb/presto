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
package com.facebook.presto.operator;

import com.facebook.airlift.discovery.client.ForDiscoveryClient;
import com.facebook.airlift.discovery.client.ServiceDescriptor;
import com.facebook.airlift.discovery.client.ServiceDescriptorsRepresentation;
import com.facebook.airlift.http.client.FullJsonResponseHandler;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.PrestoException;

import javax.inject.Inject;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.util.stream.Collectors.toSet;

public class DynamicFilterClientFactory
        implements DynamicFilterClientSupplier
{
    private final Supplier<URI> discoveryURISupplier;
    private URI coordinateUri;
    private final HttpClient httpClient;
    private final JsonCodec<DynamicFilterSummary> summaryJsonCodec;

    @Inject
    public DynamicFilterClientFactory(
            @ForDiscoveryClient Supplier<URI> discoveryURISupplier,
            @ForDynamicFilterSummary HttpClient httpClient,
            JsonCodec<DynamicFilterSummary> summaryJsonCodec)
    {
        this.discoveryURISupplier = discoveryURISupplier;
        this.httpClient = httpClient;
        this.summaryJsonCodec = summaryJsonCodec;
    }

    @Override
    public DynamicFilterClient createClient(TaskId taskId, String source, int driverId, int expectedDriversCount, TypeManager typeManager)
    {
        if (coordinateUri == null) {
            try {
                coordinateUri = new URI(parseDiscoveryServiceByHttpClient());
            }
            catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
        return new HttpDynamicFilterClient(summaryJsonCodec, coordinateUri, httpClient, Optional.of(taskId), Optional.of(source), driverId, expectedDriversCount, typeManager);
    }

    @Override
    public DynamicFilterClient createClient(TypeManager typeManager)
    {
        if (coordinateUri == null) {
            try {
                coordinateUri = new URI(parseDiscoveryServiceByHttpClient());
            }
            catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
        return new HttpDynamicFilterClient(summaryJsonCodec, coordinateUri, httpClient, Optional.empty(), Optional.empty(), -1, -1, typeManager);
    }

    private String parseDiscoveryServiceByHttpClient()
    {
        FullJsonResponseHandler.JsonResponse<ServiceDescriptorsRepresentation> httpResponse = httpClient.execute(
                prepareGet()
                        .setUri(HttpUriBuilder.uriBuilderFrom(discoveryURISupplier.get())
                                .appendPath("/v1/service")
                                .build())
                        .build(),
                createFullJsonResponseHandler(jsonCodec(ServiceDescriptorsRepresentation.class)));

        if (httpResponse.getStatusCode() == Response.Status.OK.getStatusCode()) {
            ServiceDescriptorsRepresentation serviceDescriptorsRepresentation = httpResponse.getValue();

            Set<ServiceDescriptor> coordinators = serviceDescriptorsRepresentation.getServiceDescriptors().stream()
                    .filter(serviceDescriptor -> serviceDescriptor.getProperties().getOrDefault("coordinator", "false").equals("true"))
                    .collect(toSet());

            Optional<ServiceDescriptor> selectedCoordinator = coordinators.stream().findFirst();
            return selectedCoordinator.get().getProperties().get("http-external");
        }

        throw new PrestoException(NOT_FOUND, "Coordinator not found");
    }
}
