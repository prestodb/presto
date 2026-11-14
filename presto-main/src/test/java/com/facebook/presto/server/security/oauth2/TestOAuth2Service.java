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

import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.server.security.oauth2.TokenPairSerializer.ACCESS_TOKEN_CLAIMS_ONLY_SERIALIZER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOAuth2Service
{
    private static OAuth2Service createService()
            throws IOException
    {
        return new OAuth2Service(
                new OAuth2Client()
                {
                    @Override
                    public void load() {}

                    @Override
                    public OAuth2Client.Request createAuthorizationRequest(String state, URI callbackUri)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public OAuth2Client.Response getOAuth2Response(String code, URI callbackUri, Optional<String> nonce)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Optional<Map<String, Object>> getClaims(String accessToken)
                    {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public OAuth2Client.Response refreshTokens(String refreshToken)
                    {
                        throw new UnsupportedOperationException();
                    }
                },
                new OAuth2Config(),
                new OAuth2TokenHandler()
                {
                    @Override
                    public void setAccessToken(String hashedState, String accessToken) {}

                    @Override
                    public void setTokenExchangeError(String hashedState, String errorMessage) {}
                },
                ACCESS_TOKEN_CLAIMS_ONLY_SERIALIZER,
                Optional.empty());
    }

    @Test
    public void testCallbackErrorHtmlEscapesUnknownErrorCode()
            throws IOException
    {
        String payload = "<script>alert('xss')</script>";
        String html = createService().getCallbackErrorHtml(payload);

        assertFalse(html.contains(payload), "error code was reflected into the failure page without escaping");
        assertFalse(html.contains("<script>"), "unescaped <script> tag present in the failure page");
        assertTrue(html.contains("&lt;script&gt;"), "escaped error code missing from the failure page");
    }

    @Test
    public void testCallbackErrorHtmlKeepsKnownErrorCodeMessage()
            throws IOException
    {
        String html = createService().getCallbackErrorHtml("access_denied");
        assertTrue(html.contains("OAuth2 server denied the login"), "known error message must be rendered unchanged");
    }

    @Test
    public void testInternalFailureHtmlEscapesMessage()
            throws IOException
    {
        String html = createService().getInternalFailureHtml("<img src=x onerror=alert(1)>");
        assertFalse(html.contains("<img src=x onerror=alert(1)>"), "message was reflected without escaping");
        assertTrue(html.contains("&lt;img src=x onerror=alert(1)&gt;"), "escaped message missing from the failure page");
    }
}
