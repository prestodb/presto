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

import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;

import java.util.Base64;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.Request.Builder.fromRequest;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

public class LdapRequestFilter
        implements HttpRequestFilter
{
    private final String user;
    private final String password;

    public LdapRequestFilter(String user, String password)
    {
        this.user = requireNonNull(user, "user is null");
        checkArgument(!user.contains(":"), "Illegal character ':' found in username");
        this.password = requireNonNull(password, "password is null");
    }

    @Override
    public Request filterRequest(Request request)
    {
        String value = "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(ISO_8859_1));
        return fromRequest(request)
                .addHeader(HttpHeaders.AUTHORIZATION, value)
                .build();
    }
}
