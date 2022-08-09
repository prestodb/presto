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
package com.facebook.presto.server.security.oauth2;

import com.facebook.presto.server.MockHttpServletRequest;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.UriBuilder;

import static com.facebook.presto.server.security.oauth2.OAuth2Utils.getSchemeUriBuilder;
import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;
import static org.testng.Assert.assertEquals;

public class TestOAuth2Utils
{
    @Test
    public void testGetSchemeUriBuilderNoProtoHeader()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .build(),
                "testRemote",
                   "http://www.example.com");

        UriBuilder builder = getSchemeUriBuilder(request);
        assertEquals(builder.build().getScheme(), "http");
    }

    @Test
    public void testGetSchemeUriBuilderProtoHeader()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(X_FORWARDED_PROTO, "https")
                        .build(),
                "testRemote",
                "http://www.example.com");

        UriBuilder builder = getSchemeUriBuilder(request);
        assertEquals(builder.build().getScheme(), "https");
    }
}
