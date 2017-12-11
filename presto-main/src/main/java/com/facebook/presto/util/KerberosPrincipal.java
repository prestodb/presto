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
package com.facebook.presto.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KerberosPrincipal
{
    private static final char REALM_SEPARATOR = '@';
    private static final char PARTS_SEPARATOR = '/';
    private static final char ESCAPE_CHARACTER = '\\';

    private static final String HOSTNAME_PLACEHOLDER = "_HOST";

    private final String username;
    private final Optional<String> hostname;
    private final Optional<String> realm;

    public static KerberosPrincipal valueOf(String value)
    {
        requireNonNull(value, "value is null");

        int hostnameSeparatorIndex = -1;
        int realmSeparatorIndex = -1;

        boolean escape = false;
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            switch (c) {
                case ESCAPE_CHARACTER:
                    escape = !escape;
                    break;
                case PARTS_SEPARATOR:
                    if (escape) {
                        escape = false;
                    }
                    else {
                        hostnameSeparatorIndex = index;
                    }
                    break;
                case REALM_SEPARATOR:
                    if (escape) {
                        escape = false;
                    }
                    else {
                        realmSeparatorIndex = index;
                    }
                    break;
                default:
                    escape = false;
                    break;
            }
        }

        if (hostnameSeparatorIndex >= 0 && realmSeparatorIndex >= 0 && hostnameSeparatorIndex > realmSeparatorIndex) {
            throw invalidKerberosPrincipal(value);
        }

        String usernameAndHostname;
        Optional<String> realm;
        if (realmSeparatorIndex >= 0) {
            // usernameAndHostname cannot be empty
            if (realmSeparatorIndex == 0) {
                throw invalidKerberosPrincipal(value);
            }

            // realm cannot be empty
            if (realmSeparatorIndex == value.length() - 1) {
                throw invalidKerberosPrincipal(value);
            }

            usernameAndHostname = value.substring(0, realmSeparatorIndex);
            realm = Optional.of(value.substring(realmSeparatorIndex + 1, value.length()));
        }
        else {
            usernameAndHostname = value;
            realm = Optional.empty();
        }

        String username;
        Optional<String> hostname;
        if (hostnameSeparatorIndex >= 0) {
            // username cannot be empty
            if (hostnameSeparatorIndex == 0) {
                throw invalidKerberosPrincipal(value);
            }

            // hostname cannot be empty
            if (hostnameSeparatorIndex == usernameAndHostname.length() - 1) {
                throw invalidKerberosPrincipal(value);
            }

            username = usernameAndHostname.substring(0, hostnameSeparatorIndex);
            hostname = Optional.of(usernameAndHostname.substring(hostnameSeparatorIndex + 1, usernameAndHostname.length()));
        }
        else {
            username = usernameAndHostname;
            hostname = Optional.empty();
        }

        return new KerberosPrincipal(username, hostname, realm);
    }

    private static IllegalArgumentException invalidKerberosPrincipal(String value)
    {
        return new IllegalArgumentException("Invalid Kerberos principal: " + value);
    }

    public KerberosPrincipal(String userName, Optional<String> hostName, Optional<String> realm)
    {
        this.username = requireNonNull(userName, "username is null");
        this.hostname = requireNonNull(hostName, "hostname is null");
        this.realm = requireNonNull(realm, "realm is null");
    }

    public String getUserName()
    {
        return username;
    }

    public Optional<String> getHostName()
    {
        return hostname;
    }

    public Optional<String> getRealm()
    {
        return realm;
    }

    public KerberosPrincipal substituteHostnamePlaceholder()
    {
        return substituteHostnamePlaceholder(getLocalCanonicalHostName());
    }

    public KerberosPrincipal substituteHostnamePlaceholder(String hostname)
    {
        if (this.hostname.isPresent() && this.hostname.get().equals(HOSTNAME_PLACEHOLDER)) {
            return new KerberosPrincipal(username, Optional.of(hostname), realm);
        }
        return this;
    }

    private static String getLocalCanonicalHostName()
    {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(username);
        hostname.ifPresent(hostname -> builder.append(PARTS_SEPARATOR).append(hostname));
        realm.ifPresent(realm -> builder.append(REALM_SEPARATOR).append(realm));
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KerberosPrincipal that = (KerberosPrincipal) o;
        return Objects.equals(username, that.username) &&
                Objects.equals(hostname, that.hostname) &&
                Objects.equals(realm, that.realm);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(username, hostname, realm);
    }
}
