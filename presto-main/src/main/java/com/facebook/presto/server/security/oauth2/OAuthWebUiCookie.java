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

import jakarta.ws.rs.core.NewCookie;
import jakarta.ws.rs.core.NewCookie.SameSite;

import java.time.Instant;
import java.util.Date;

import static jakarta.ws.rs.core.Cookie.DEFAULT_VERSION;
import static jakarta.ws.rs.core.NewCookie.DEFAULT_MAX_AGE;

public final class OAuthWebUiCookie
{
    // prefix according to: https://tools.ietf.org/html/draft-ietf-httpbis-rfc6265bis-05#section-4.1.3.1
    public static final String OAUTH2_COOKIE = "__Secure-Presto-OAuth2-Token";

    public static final String API_PATH = "/";

    private OAuthWebUiCookie() {}

    public static NewCookie create(String token, Instant tokenExpiration)
    {
        return new NewCookie.Builder(OAUTH2_COOKIE)
                .value(token)
                .path(API_PATH)
                .version(DEFAULT_VERSION)
                .maxAge(DEFAULT_MAX_AGE)
                .expiry(Date.from(tokenExpiration))
                .secure(true)
                .httpOnly(true)
                .sameSite(SameSite.LAX)
                .build();
    }

    public static NewCookie delete()
    {
        return new NewCookie.Builder(OAUTH2_COOKIE)
                .value("delete")
                .path(API_PATH)
                .version(DEFAULT_VERSION)
                .maxAge(0)
                .secure(true)
                .httpOnly(true)
                .sameSite(SameSite.LAX)
                .build();
    }
}
