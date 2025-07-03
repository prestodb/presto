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

import java.util.Arrays;

public enum OAuth2ErrorCode
{
    ACCESS_DENIED("access_denied", "OAuth2 server denied the login"),
    UNAUTHORIZED_CLIENT("unauthorized_client", "OAuth2 server does not allow request from this Presto server"),
    SERVER_ERROR("server_error", "OAuth2 server had a failure"),
    TEMPORARILY_UNAVAILABLE("temporarily_unavailable", "OAuth2 server is temporarily unavailable");

    private final String code;
    private final String message;

    OAuth2ErrorCode(String code, String message)
    {
        this.code = code;
        this.message = message;
    }

    public static OAuth2ErrorCode fromString(String codeStr)
    {
        return Arrays.stream(OAuth2ErrorCode.values())
                .filter(value -> codeStr.equalsIgnoreCase(value.code))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No enum constant " + OAuth2ErrorCode.class.getCanonicalName() + "." + codeStr));
    }

    public String getMessage()
    {
        return this.message;
    }
}
