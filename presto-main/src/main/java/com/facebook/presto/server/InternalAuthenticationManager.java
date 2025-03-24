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

import com.facebook.airlift.http.client.HttpRequestFilter;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.security.BasicPrincipal;
import com.google.common.hash.Hashing;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.security.Principal;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;

import static com.facebook.airlift.http.client.Request.Builder.fromRequest;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    public static final String PRESTO_INTERNAL_BEARER = "X-Presto-Internal-Bearer";
    private static final Logger log = Logger.get(InternalAuthenticationManager.class);

    private final boolean internalJwtEnabled;
    private final byte[] hmac;
    private final String nodeId;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo)
    {
        this(internalCommunicationConfig.getSharedSecret(), nodeInfo.getNodeId(), internalCommunicationConfig.isInternalJwtEnabled());
    }

    public InternalAuthenticationManager(Optional<String> sharedSecret, String nodeId, boolean internalJwtEnabled)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        this.internalJwtEnabled = internalJwtEnabled;
        if (internalJwtEnabled) {
            this.hmac = Hashing.sha256().hashString(sharedSecret.get(), UTF_8).asBytes();
        }
        else {
            this.hmac = null;
        }
        this.nodeId = nodeId;
    }

    public boolean isInternalJwtEnabled()
    {
        return internalJwtEnabled;
    }

    private String generateJwt()
    {
        return Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, hmac)
                .setSubject(nodeId)
                .setExpiration(Date.from(ZonedDateTime.now().plusMinutes(5).toInstant()))
                .compact();
    }

    private String parseJwt(String jwt)
    {
        return Jwts.parser()
                .setSigningKey(hmac)
                .parseClaimsJws(jwt)
                .getBody()
                .getSubject();
    }

    public boolean isInternalRequest(ContainerRequestContext context)
    {
        return context.getHeaderString(PRESTO_INTERNAL_BEARER) != null;
    }

    public Principal authenticateInternalRequest(ContainerRequestContext context)
    {
        if (!internalJwtEnabled) {
            log.error("Internal authentication in not enabled");
            return null;
        }

        String internalBearer = context.getHeaderString(PRESTO_INTERNAL_BEARER);

        if (internalBearer == null) {
            log.error("Internal authentication failed");
            return null;
        }

        try {
            String subject = parseJwt(internalBearer);
            return new BasicPrincipal(subject);
        }
        catch (JwtException e) {
            log.error(e, "Internal authentication failed");
            return null;
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }

    @Override
    public Request filterRequest(Request request)
    {
        if (!internalJwtEnabled) {
            return request;
        }

        return fromRequest(request)
                .addHeader(PRESTO_INTERNAL_BEARER, generateJwt())
                .build();
    }
}
