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

import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.NewCookie;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static com.facebook.presto.server.security.oauth2.OAuth2CallbackResource.CALLBACK_ENDPOINT;
import static com.google.common.base.Predicates.not;
import static jakarta.ws.rs.core.Cookie.DEFAULT_VERSION;
import static jakarta.ws.rs.core.NewCookie.DEFAULT_MAX_AGE;

public final class NonceCookie
{
    // prefix according to: https://tools.ietf.org/html/draft-ietf-httpbis-rfc6265bis-05#section-4.1.3.1
    public static final String NONCE_COOKIE = "__Secure-Presto-Nonce";

    private NonceCookie() {}

    public static NewCookie create(String nonce, Instant tokenExpiration)
    {
        return new NewCookie(
                NONCE_COOKIE,
                nonce,
                CALLBACK_ENDPOINT,
                null,
                DEFAULT_VERSION,
                null,
                DEFAULT_MAX_AGE,
                Date.from(tokenExpiration),
                true,
                true);
    }

    public static jakarta.servlet.http.Cookie createServletCookie(String nonce, Instant tokenExpiration)
    {
        return toServletCookie(create(nonce, tokenExpiration));
    }

    public static jakarta.servlet.http.Cookie toServletCookie(NewCookie cookie)
    {
        jakarta.servlet.http.Cookie servletCookie = new jakarta.servlet.http.Cookie(cookie.getName(), cookie.getValue());
        servletCookie.setPath(cookie.getPath());
        servletCookie.setMaxAge(cookie.getMaxAge());
        servletCookie.setSecure(cookie.isSecure());
        servletCookie.setHttpOnly(cookie.isHttpOnly());

        return servletCookie;
    }

    public static Optional<String> read(Cookie cookie)
    {
        return Optional.ofNullable(cookie)
                .map(Cookie::getValue)
                .filter(not(StringUtils::isBlank));
    }

    public static NewCookie delete()
    {
        return new NewCookie(
                NONCE_COOKIE,
                "delete",
                CALLBACK_ENDPOINT,
                null,
                DEFAULT_VERSION,
                null,
                0,
                null,
                true,
                true);
    }
}
