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
package com.facebook.presto.server.security;

import com.facebook.presto.spi.security.AuthorizedIdentity;

import javax.servlet.http.HttpServletRequest;

import java.util.Optional;

public class ServletSecurityUtils
{
    public static final String AUTHORIZED_IDENTITY_ATTRIBUTE = "presto.authorized-identity";

    private ServletSecurityUtils() {}

    public static void setAuthorizedIdentity(HttpServletRequest servletRequest, AuthorizedIdentity authorizedIdentity)
    {
        servletRequest.setAttribute(AUTHORIZED_IDENTITY_ATTRIBUTE, authorizedIdentity);
    }

    public static Optional<AuthorizedIdentity> authorizedIdentity(HttpServletRequest servletRequest)
    {
        return Optional.ofNullable((AuthorizedIdentity) servletRequest.getAttribute(AUTHORIZED_IDENTITY_ATTRIBUTE));
    }
}
