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

import javax.ws.rs.core.NewCookie;

import java.time.Instant;
import java.util.Date;

import static javax.ws.rs.core.Cookie.DEFAULT_VERSION;
import static javax.ws.rs.core.NewCookie.DEFAULT_MAX_AGE;

public final class OAuthWebUiCookie
{
    // prefix according to: https://tools.ietf.org/html/draft-ietf-httpbis-rfc6265bis-05#section-4.1.3.1
    public static final String OAUTH2_COOKIE = "__Secure-Presto-OAuth2-Token";

    public static final String API_PATH = "/";

    private OAuthWebUiCookie() {}

    public static NewCookie create(String token, Instant tokenExpiration)
    {
        return new NewCookie(
                OAUTH2_COOKIE,
                token,
                API_PATH,
                null,
                DEFAULT_VERSION,
                null,
                DEFAULT_MAX_AGE,
                Date.from(tokenExpiration),
                true,
                true);
    }
    public static NewCookie delete()
    {
        return new NewCookie(
                OAUTH2_COOKIE,
                "delete",
                API_PATH,
                null,
                DEFAULT_VERSION,
                null,
                0,
                null,
                true,
                true);
    }
}
