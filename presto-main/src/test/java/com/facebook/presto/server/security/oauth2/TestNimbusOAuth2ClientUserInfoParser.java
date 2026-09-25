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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.units.Duration;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.openid.connect.sdk.UserInfoErrorResponse;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.UserInfoSuccessResponse;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestNimbusOAuth2ClientUserInfoParser
{
    private static final String CLIENT_ID = "test-client-id";
    private static final String ADDITIONAL_AUDIENCE = "additional-audience";

    private NimbusOAuth2Client createClient(String principalField, String additionalAudiences)
    {
        OAuth2Config config = new OAuth2Config()
                .setIssuer("https://issuer.example.com")
                .setClientId(CLIENT_ID)
                .setClientSecret("test-secret")
                .setPrincipalField(principalField)
                .setAdditionalAudiences(additionalAudiences)
                .setMaxClockSkew(new Duration(1, MINUTES));

        OAuth2ServerConfigProvider configProvider = new OAuth2ServerConfigProvider()
        {
            @Override
            public OAuth2ServerConfig get()
            {
                return new OAuth2ServerConfig(
                        Optional.empty(),
                        URI.create("https://issuer.example.com/auth"),
                        URI.create("https://issuer.example.com/token"),
                        URI.create("https://issuer.example.com/jwks"),
                        Optional.empty());
            }
        };

        HttpClient httpClient = new TestingHttpClient(request -> {
            throw new UnsupportedOperationException("HTTP client should not be called in these tests");
        });
        NimbusHttpClient nimbusHttpClient = new NimbusAirliftHttpClient(httpClient);

        return new NimbusOAuth2Client(config, configProvider, nimbusHttpClient);
    }

    @Test
    public void testParseSuccessWithStandardSubClaim()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("email", "user@example.com");
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
        UserInfoSuccessResponse successResponse = response.toSuccessResponse();
        assertThat(successResponse.getUserInfo().getSubject().getValue()).isEqualTo("user@example.com");
    }

    @Test
    public void testParseSuccessWithCustomPrincipalField()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("email", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        httpResponse.setContentType("application/json");
        JSONObject body = new JSONObject();
        body.put("email", "custom@example.com");
        body.put("name", "Custom User");
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
        UserInfoSuccessResponse successResponse = response.toSuccessResponse();
        // The parser should add "sub" claim with the value from "email"
        assertThat(successResponse.getUserInfo().getSubject().getValue()).isEqualTo("custom@example.com");
    }

    @Test
    public void testParseSuccessWithCustomPrincipalFieldAndExistingSub()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("email", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "original-sub");
        body.put("email", "custom@example.com");
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
        UserInfoSuccessResponse successResponse = response.toSuccessResponse();
        // Should keep the original "sub" claim
        assertThat(successResponse.getUserInfo().getSubject().getValue()).isEqualTo("original-sub");
    }

    @Test
    public void testParseMissingPrincipalField()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("email", "user@example.com");
        // Missing "sub" field
        httpResponse.setBody(body.toJSONString());

        assertThatThrownBy(() -> client.parse(httpResponse))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("/userinfo response missing principal field sub");
    }

    @Test
    public void testParseMissingCustomPrincipalField()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("custom_field", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        // Missing "custom_field"
        httpResponse.setBody(body.toJSONString());

        assertThatThrownBy(() -> client.parse(httpResponse))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("/userinfo response missing principal field custom_field");
    }

    @Test
    public void testParseWithValidAudienceString()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("aud", CLIENT_ID);
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    @Test
    public void testParseWithValidAudienceArray()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        JSONArray audiences = new JSONArray();
        audiences.add(CLIENT_ID);
        audiences.add("other-audience");
        body.put("aud", audiences);
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    @Test
    public void testParseWithValidAdditionalAudience()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("aud", ADDITIONAL_AUDIENCE);
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    @Test
    public void testParseWithInvalidAudienceString()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("aud", "invalid-audience");
        httpResponse.setBody(body.toJSONString());

        assertThatThrownBy(() -> client.parse(httpResponse))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Invalid audience in /userinfo response");
    }

    @Test
    public void testParseWithInvalidAudienceArray()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        JSONArray audiences = new JSONArray();
        audiences.add("invalid-audience-1");
        audiences.add("invalid-audience-2");
        body.put("aud", audiences);
        httpResponse.setBody(body.toJSONString());

        assertThatThrownBy(() -> client.parse(httpResponse))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Invalid audience in /userinfo response");
    }

    @Test
    public void testParseWithMixedAudienceArrayContainingValid()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        JSONArray audiences = new JSONArray();
        audiences.add("invalid-audience");
        audiences.add(CLIENT_ID);
        body.put("aud", audiences);
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    @Test
    public void testParseWithUnsupportedAudienceType()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("aud", 12345); // Integer instead of String or Array
        httpResponse.setBody(body.toJSONString());

        assertThatThrownBy(() -> client.parse(httpResponse))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Unsupported 'aud' claim type in /userinfo response");
    }

    @Test
    public void testParseWithNoAudienceClaim()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        // No "aud" claim - should be allowed
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    @Test
    public void testParseWithAudienceArrayContainingNonStrings()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        JSONArray audiences = new JSONArray();
        audiences.add(CLIENT_ID);
        audiences.add(123); // Non-string element should be filtered out
        audiences.add(null); // Null element should be filtered out
        body.put("aud", audiences);
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    @Test
    public void testParseErrorResponse()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = new HTTPResponse(401);
        httpResponse.setContentType("application/json");
        JSONObject body = new JSONObject();
        body.put("error", "invalid_token");
        body.put("error_description", "The access token is invalid");
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);
        assertThat(response.indicatesSuccess()).isFalse();
        UserInfoErrorResponse errorResponse = response.toErrorResponse();
        assertThat(errorResponse.getErrorObject().getCode()).isEqualTo("invalid_token");
        assertThat(errorResponse.getErrorObject().getDescription()).isEqualTo("The access token is invalid");
    }

    @Test
    public void testParseWithEmptyAudienceArray()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", ADDITIONAL_AUDIENCE);
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("aud", new JSONArray()); // Empty array
        httpResponse.setBody(body.toJSONString());

        assertThatThrownBy(() -> client.parse(httpResponse))
                .isInstanceOf(ParseException.class)
                .hasMessageContaining("Invalid audience in /userinfo response");
    }

    @Test
    public void testParseWithMultipleAdditionalAudiences()
            throws Exception
    {
        NimbusOAuth2Client client = createClient("sub", "aud1,aud2,aud3");
        HTTPResponse httpResponse = createSuccessResponse();
        JSONObject body = new JSONObject();
        body.put("sub", "user@example.com");
        body.put("aud", "aud2"); // One of the additional audiences
        httpResponse.setBody(body.toJSONString());

        UserInfoResponse response = client.parse(httpResponse);

        assertThat(response.indicatesSuccess()).isTrue();
    }

    private HTTPResponse createSuccessResponse() throws ParseException
    {
        HTTPResponse response = new HTTPResponse(200);
        response.setContentType("application/json");
        return response;
    }
}

// Made with Bob
