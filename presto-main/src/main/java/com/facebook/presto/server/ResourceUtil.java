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
package com.facebook.presto.server;

import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimeZoneNotSupportedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.Locale;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static java.lang.String.format;

final class ResourceUtil
{
    private ResourceUtil()
    {
    }

    public static Session createSessionForRequest(HttpServletRequest servletRequest)
    {
        SessionBuilder sessionBuilder = Session.builder()
                .setUser(getRequiredHeader(servletRequest, PRESTO_USER, "User"))
                .setSource(servletRequest.getHeader(PRESTO_SOURCE))
                .setCatalog(getRequiredHeader(servletRequest, PRESTO_CATALOG, "Catalog"))
                .setSchema(getRequiredHeader(servletRequest, PRESTO_SCHEMA, "Schema"))
                .setRemoteUserAddress(servletRequest.getRemoteAddr())
                .setUserAgent(servletRequest.getHeader(USER_AGENT));

        String timeZoneId = servletRequest.getHeader(PRESTO_TIME_ZONE);
        if (timeZoneId != null) {
            sessionBuilder.setTimeZoneKey(getTimeZoneKey(timeZoneId));
        }

        String language = servletRequest.getHeader(PRESTO_LANGUAGE);
        if (language != null) {
            sessionBuilder.setLocale(Locale.forLanguageTag(language));
        }

        return sessionBuilder.build();
    }

    private static String getRequiredHeader(HttpServletRequest servletRequest, String name, String description)
    {
        String user = servletRequest.getHeader(name);
        assertRequest(!isNullOrEmpty(user), description + " (%s) is empty", PRESTO_USER);
        return user;
    }

    public static void assertRequest(boolean expression, String format, Object... args)
    {
        if (!expression) {
            throw badRequest(format(format, args));
        }
    }

    private static TimeZoneKey getTimeZoneKey(String timeZoneId)
    {
        try {
            return TimeZoneKey.getTimeZoneKey(timeZoneId);
        }
        catch (TimeZoneNotSupportedException e) {
            throw badRequest(e.getMessage());
        }
    }

    private static WebApplicationException badRequest(String message)
    {
        throw new WebApplicationException(Response
                .status(Status.BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(message)
                .build());
    }
}
