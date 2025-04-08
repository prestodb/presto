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

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtParserBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Deserializer;
import io.jsonwebtoken.io.Serializer;
import io.jsonwebtoken.jackson.io.JacksonDeserializer;
import io.jsonwebtoken.jackson.io.JacksonSerializer;

import java.util.Map;

// avoid reflection and services lookup
public final class JwtUtil
{
    private static final Serializer<Map<String, ?>> JWT_SERIALIZER = new JacksonSerializer<>();
    private static final Deserializer<Map<String, ?>> JWT_DESERIALIZER = new JacksonDeserializer<>();

    private JwtUtil() {}

    public static JwtBuilder newJwtBuilder()
    {
        return Jwts.builder()
                .serializeToJsonWith(JWT_SERIALIZER);
    }

    public static JwtParserBuilder newJwtParserBuilder()
    {
        return Jwts.parserBuilder()
                .deserializeJsonWith(JWT_DESERIALIZER);
    }
}
