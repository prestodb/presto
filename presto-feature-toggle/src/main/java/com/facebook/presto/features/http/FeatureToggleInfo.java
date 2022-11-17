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
package com.facebook.presto.features.http;

import com.facebook.presto.features.binder.Feature;
import com.facebook.presto.features.binder.PrestoFeatureToggle;
import com.google.inject.Inject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static com.facebook.presto.features.http.FeatureInfo.getActiveFeatureInfo;
import static com.facebook.presto.features.http.FeatureInfo.getInitialFeatureInfo;
import static com.facebook.presto.features.http.FeatureInfo.getOverrideFeatureInfo;

@Path("/v1/features")
public class FeatureToggleInfo
{
    private final PrestoFeatureToggle featureToggle;

    @Inject
    public FeatureToggleInfo(PrestoFeatureToggle featureToggle)
    {
        this.featureToggle = featureToggle;
    }

    /**
     * HTTP endpoint for retrieving list of all defined feature toggles
     * <p>
     * endpoint accepts two query params:
     *     <ul>
     *         <li>enabled (boolean)</li>
     *         <li>featureId (String)</li>
     *     </ul>
     * <p>
     * example: no params -> returns list of all feature toggles
     * <pre>{@code
     *     https://server:port/v1/features
     * }</pre>
     * response:
     * <pre>{@code
     * [
     *   {
     *     "featureId": "query-rate-limiter",
     *     "enabled": true,
     *     "featureClass": "com.facebook.presto.server.protocol.QueryRateLimiter",
     *     "featureInstances": [
     *       "com.facebook.presto.server.protocol.QueryBlockingRateLimiter",
     *       "com.facebook.presto.server.protocol.AnotherQueryBlockingRateLimiter"
     *     ],
     *     "currentInstance": "com.facebook.presto.server.protocol.AnotherQueryBlockingRateLimiter",
     *     "defaultInstance": "com.facebook.presto.server.protocol.QueryBlockingRateLimiter"
     *   },
     *   {
     *     "featureId": "query-logger",
     *     "enabled": false,
     *     "featureInstances": []
     *   },
     *   {
     *     "featureId": "circuit-breaker",
     *     "enabled": true,
     *     "featureClass": "com.facebook.presto.server.protocol.RetryCircuitBreakerInt",
     *     "featureInstances": [
     *       "com.facebook.presto.server.protocol.RetryCircuitBreaker"
     *     ],
     *     "currentInstance": "com.facebook.presto.server.protocol.RetryCircuitBreaker",
     *     "defaultInstance": "com.facebook.presto.server.protocol.RetryCircuitBreaker"
     *   },
     *   {
     *     "featureId": "query-cancel",
     *     "enabled": true,
     *     "featureInstances": [],
     *     "strategy": {
     *       "active": true,
     *       "strategyName": "AllowList",
     *       "config": {
     *         "strategy": "AllowList",
     *         "allow-list-user": ".*bane",
     *         "allow-list-source": ".*cli.*"
     *       }
     *     }
     *   },
     *   {
     *     "featureId": "memory-feature",
     *     "enabled": true,
     *     "featureClass": "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterface",
     *     "featureInstances": [
     *       "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterfaceImpl"
     *     ],
     *     "currentInstance": "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterfaceImpl",
     *     "defaultInstance": "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterfaceImpl"
     *   }
     * ]
     * }</pre>
     * example filter by "featureId": returns list of all feature toggles where feature id contains given featureId
     * <pre>{@code
     *     https://server:port/v1/features?featureId=memory
     * }</pre>
     * response:
     * <pre>{@code
     * [
     *   {
     *     "featureId": "memory-feature",
     *     "enabled": true,
     *     "featureClass": "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterface",
     *     "featureInstances": [
     *       "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterfaceImpl"
     *     ],
     *     "currentInstance": "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterfaceImpl",
     *     "defaultInstance": "com.facebook.presto.memory.context.ft.MemoryFeatureToggleInterfaceImpl"
     *   }
     * ]
     * }</pre>
     * example filter by "enabled":
     * <pre>{@code
     *     https://server:port/v1/features?enabled=false
     * }</pre>
     * response:
     * <pre>{@code
     * [
     *   {
     *     "featureId": "query-logger",
     *     "enabled": false,
     *     "featureInstances": []
     *   }
     * ]
     * }</pre>
     * <p>
     *
     * @param enabled the query param, if given filters result list by enabled status
     * @param featureId the query param, if given filters result list by feature id (or part of the feature id)
     * @returns returns JSON response with list of filtered feature toggles
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response info(@QueryParam("enabled") Boolean enabled, @QueryParam("featureId") String featureId)
    {
        List<FeatureInfo> infoList = FeatureInfo.featureInfos(featureToggle);
        if (enabled != null) {
            infoList = infoList.stream()
                    .filter(feature -> {
                        if (enabled) {
                            return feature.isEnabled();
                        }
                        else {
                            return !feature.isEnabled();
                        }
                    })
                    .collect(Collectors.toList());
        }
        if (featureId != null) {
            infoList = infoList.stream()
                    .filter(f -> f.getFeatureId().toLowerCase(Locale.US).contains(featureId.toLowerCase(Locale.US)))
                    .collect(Collectors.toList());
        }
        return Response.ok().entity(infoList).build();
    }

    /**
     * Returns detailed information about feature toggle with given feature id
     * <p>
     * endpoint accepts one path param:
     *     <ul>
     *         <li>featureId (String)</li>
     *     </ul>
     * <p>
     * response contains three objects:
     *     <ul>
     *         <li>initialConfiguration : initial configuration defined in featureBinder</li>
     *         <li>configurationOverride : configuration override in feature toggle configuration file (source></li>
     *         <li>activeConfiguration : active feature toggle configuration</li>
     *     </ul>
     * <p>
     * example:
     * <pre>{@code
     *     https://server:port/v1/features/query-cancel
     * }</pre>
     * response:
     * <pre>{@code
     * {
     *   "initialConfiguration": {
     *     "featureId": "query-cancel",
     *     "enabled": true,
     *     "strategy": {
     *       "active": true,
     *       "strategyName": "AllowList",
     *       "config": {
     *         "allow-list-user": ".*prestodb",
     *         "allow-list-source": ".*IDEA.*"
     *       }
     *     }
     *   },
     *   "configurationOverride": {
     *     "featureId": "query-cancel",
     *     "enabled": true,
     *     "strategy": {
     *       "active": true,
     *       "strategyName": "AllowList",
     *       "config": {
     *         "strategy": "AllowList",
     *         "allow-list-user": ".*bane",
     *         "allow-list-source": ".*cli.*"
     *       }
     *     }
     *   },
     *   "activeConfiguration": {
     *     "featureId": "query-cancel",
     *     "enabled": true,
     *     "featureInstances": [],
     *     "strategy": {
     *       "active": true,
     *       "strategyName": "AllowList",
     *       "config": {
     *         "strategy": "AllowList",
     *         "allow-list-user": ".*bane",
     *         "allow-list-source": ".*cli.*"
     *       }
     *     }
     *   }
     * }
     * }</pre>
     *
     * @param featureId the query param: the feature id
     * @return the JSON response with detailed information about feature toggle
     */
    @GET
    @Path("/{featureId}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response details(@PathParam("featureId") String featureId)
    {
        if (featureToggle.getFeatureMap().get(featureId) != null) {
            Feature<?> feature = featureToggle.getFeatureMap().get(featureId);
            FeatureInfo initialFeatureInfo = getInitialFeatureInfo(feature);
            FeatureInfo overrideFeatureInfo = getOverrideFeatureInfo(feature, featureToggle);
            FeatureInfo activeFeatureInfo = getActiveFeatureInfo(feature, featureToggle);
            return Response.ok().entity(new FeatureDetails(initialFeatureInfo, overrideFeatureInfo, activeFeatureInfo)).build();
        }
        return Response.noContent().build();
    }
}
