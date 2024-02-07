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

package com.facebook.presto.sessionpropertyproviders;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.SessionPropertyMetadata;
import com.facebook.presto.spi.session.SystemSessionPropertyProvider;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;

public class NativeSystemSessionPropertyProvider
        implements SystemSessionPropertyProvider
{
    public static final String SESSION_PROPERTY_ENDPOINT = "/v1/sessionProperties";

    private final Logger log = Logger.get(NativeSystemSessionPropertyProvider.class);

    private final OkHttpClient httpClient = new OkHttpClient.Builder().build();

    private final NodeManager nodeManager;

    public NativeSystemSessionPropertyProvider(NodeManager manager)
    {
        this.nodeManager = manager;
    }

    // TODO : Access specifier need made private once NodeManager is made working.
    public static List<SessionPropertyMetadata> deserializeSessionProperties(String responseBody)
    {
        JsonObjectMapperProvider objectMapperProvider = new JsonObjectMapperProvider();
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        JsonCodec<List<SessionPropertyMetadata>> sessionPropertiesJsonCodec = codecFactory.listJsonCodec(SessionPropertyMetadata.class);
        try {
            return sessionPropertiesJsonCodec.fromJson(responseBody);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to deserialize the RPC response body.");
        }
    }

    private URL getSessionPropertiesUrl()
    {
        Set<Node> nodes = nodeManager.getAllNodes();
        Node nativeNode = nodes.stream()
                .filter(node -> !node.isCoordinatorSidecar())
                .findFirst()
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Failed to find native node"));
        try {
            // endpoint to retrieve session properties from native worker
            return new URL(nativeNode.getHttpUri().toString() + SESSION_PROPERTY_ENDPOINT);
        }
        catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SessionPropertyMetadata> getSessionProperties()
    {
        // Make RPC call and get session properties from prestissimo.
        try {
            Request request = new Request.Builder()
                    .url(getSessionPropertiesUrl())
                    .addHeader("CONTENT_TYPE", "APPLICATION_JSON")
                    .build();
            String responseBody = httpClient.newCall(request).execute().body().string();

            if (responseBody.contains("sessionProperties")) {
                log.info("Native session properties from HTTP call: %s", responseBody);
            }
            return deserializeSessionProperties(responseBody);
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to get function definition for NativeSystemSessionPropertyProvider." + e.getMessage());
        }
    }
}
