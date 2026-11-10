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
import org.testng.annotations.Test;

import java.time.Instant;

import static com.facebook.presto.server.security.oauth2.NonceCookie.NONCE_COOKIE;
import static com.facebook.presto.server.security.oauth2.OAuthWebUiCookie.OAUTH2_COOKIE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestOAuth2Cookies
{
    private static final Instant EXPIRATION = Instant.now().plusSeconds(3600);

    @Test
    public void testTokenCookie()
    {
        NewCookie cookie = OAuthWebUiCookie.create("token", EXPIRATION);

        assertEquals(cookie.getName(), OAUTH2_COOKIE);
        assertEquals(cookie.getPath(), "/");
        assertSecureLaxCookie(cookie);
    }

    @Test
    public void testTokenCookieDelete()
    {
        NewCookie cookie = OAuthWebUiCookie.delete();

        assertEquals(cookie.getName(), OAUTH2_COOKIE);
        assertEquals(cookie.getMaxAge(), 0);
        assertSecureLaxCookie(cookie);
    }

    @Test
    public void testNonceCookie()
    {
        NewCookie cookie = NonceCookie.create("nonce", EXPIRATION);

        assertEquals(cookie.getName(), NONCE_COOKIE);
        assertEquals(cookie.getPath(), "/oauth2/callback");
        assertSecureLaxCookie(cookie);
    }

    @Test
    public void testNonceCookieDelete()
    {
        NewCookie cookie = NonceCookie.delete();

        assertEquals(cookie.getName(), NONCE_COOKIE);
        assertEquals(cookie.getPath(), "/oauth2/callback");
        assertEquals(cookie.getMaxAge(), 0);
        assertSecureLaxCookie(cookie);
    }

    @Test
    public void testServletCookiePreservesAttributes()
    {
        jakarta.servlet.http.Cookie nonce = NonceCookie.createServletCookie("nonce", EXPIRATION);
        assertEquals(nonce.getName(), NONCE_COOKIE);
        assertEquals(nonce.getPath(), "/oauth2/callback");
        assertTrue(nonce.getSecure());
        assertTrue(nonce.isHttpOnly());
        assertEquals(nonce.getAttribute("SameSite"), "Lax");

        // token cookie refreshed through the servlet API (OAuth2WebUiAuthenticationManager)
        jakarta.servlet.http.Cookie token = NonceCookie.toServletCookie(OAuthWebUiCookie.create("token", EXPIRATION));
        assertEquals(token.getName(), OAUTH2_COOKIE);
        assertEquals(token.getPath(), "/");
        assertTrue(token.getSecure());
        assertTrue(token.isHttpOnly());
        assertEquals(token.getAttribute("SameSite"), "Lax");
    }

    @Test
    public void testServletCookieWithoutSameSite()
    {
        NewCookie cookie = new NewCookie.Builder("name").value("value").build();

        assertNull(NonceCookie.toServletCookie(cookie).getAttribute("SameSite"));
    }

    private static void assertSecureLaxCookie(NewCookie cookie)
    {
        assertTrue(cookie.isSecure());
        assertTrue(cookie.isHttpOnly());
        assertEquals(cookie.getSameSite(), SameSite.LAX);
    }
}
