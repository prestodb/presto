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
package com.facebook.presto.client;

import okhttp3.OkHttpClient;

import static java.util.Objects.requireNonNull;

public final class StatementClientFactory
{
    public static final ProtocolVersion DEFAULT_PROTOCOL_VERSION = ProtocolVersion.V1;

    private StatementClientFactory() {}

    public static StatementClient newStatementClient(OkHttpClient httpClient, ClientSession session, String query)
    {
        return newStatementClient(DEFAULT_PROTOCOL_VERSION, httpClient, session, query);
    }

    public static StatementClient newStatementClient(ProtocolVersion protocolVersion, OkHttpClient httpClient, ClientSession session, String query)
    {
        requireNonNull(protocolVersion, "protocolVersion is null");
        switch (protocolVersion) {
            case V1:
                return new StatementClientV1(httpClient, session, query);
            case V2:
                return new StatementClientV2(httpClient, session, query);
            default:
                throw new IllegalArgumentException("Unknown protocol version: " + protocolVersion);
        }
    }
}
