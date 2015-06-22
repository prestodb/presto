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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sun.security.auth.UnixPrincipal;
import io.airlift.json.JsonCodec;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;

import static io.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.server.security.Identity.Type;
import static com.facebook.presto.server.security.Identity.Type.KERBEROS_PRINCIPAL;
import static com.facebook.presto.server.security.Identity.Type.USER_NAME;

public class DelegationToken
{
    private static final JsonCodec<DelegationToken> jsonCodec = jsonCodec(DelegationToken.class);
    private Identity identity;

    @JsonProperty
    public Identity getIdentity()
    {
        return identity;
    }

    @JsonProperty
    public DelegationToken setIdentity(Identity identity)
    {
        this.identity = new Identity().setName(identity.getName()).setType(identity.getType());
        return this;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        DelegationToken o = (DelegationToken) obj;
        return Objects.equal(this.identity, o.identity);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(identity);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("identity", identity)
                .toString();
    }

    public static DelegationToken fromJson(String jsonValue)
    {
        return jsonValue == null ? null : jsonCodec.fromJson(jsonValue);
    }

    public static String toJson(DelegationToken token)
    {
        return token == null ? null : jsonCodec.toJson(token);
    }

    public static String toJson(Principal principal)
    {
        if (principal == null) {
            return null;
        }

        String name = principal.getName();
        Type type;
        if (principal instanceof KerberosPrincipal) {
            type = KERBEROS_PRINCIPAL;
        }
        else if (principal instanceof UnixPrincipal) {
            type = USER_NAME;
        }
        else {
            throw new UnsupportedOperationException("Unsupported principal type: " + principal.toString());
        }
        return "{\"identity\":{\"name\": \"" + name + "\", \"type\": \"" + type.getValue() + "\"}}";
    }
}
