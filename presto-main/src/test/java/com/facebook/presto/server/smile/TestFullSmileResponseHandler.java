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
package com.facebook.presto.server.smile;

import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.testing.TestingResponse;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.json.smile.SmileCodecFactory;
import com.facebook.presto.server.smile.FullSmileResponseHandler.SmileResponse;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.net.MediaType;
import org.testng.annotations.Test;

import static com.facebook.airlift.http.client.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.testing.TestingResponse.contentType;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFullSmileResponseHandler
{
    private static final MediaType MEDIA_TYPE_SMILE = MediaType.create("application", "x-jackson-smile");

    private final SmileCodecFactory codecFactory = new SmileCodecFactory();
    private final SmileCodec<User> codec = codecFactory.smileCodec(User.class);
    private final FullSmileResponseHandler<User> handler = createFullSmileResponseHandler(codec);

    @Test
    public void testValidSmile()
    {
        User user = new User("Joe", 25);
        byte[] smileBytes = codec.toBytes(user);
        SmileResponse<User> response = handler.handle(null, mockResponse(OK, MEDIA_TYPE_SMILE, smileBytes));

        assertTrue(response.hasValue());
        assertEquals(response.getSmileBytes(), smileBytes);
        assertEquals(response.getValue().getName(), user.getName());
        assertEquals(response.getValue().getAge(), user.getAge());

        assertNotSame(response.getSmileBytes(), response.getSmileBytes());
        assertNotSame(response.getResponseBytes(), response.getResponseBytes());

        assertEquals(response.getResponseBytes(), response.getSmileBytes());
    }

    @Test
    public void testInvalidSmile()
    {
        byte[] invalidSmileBytes = "test".getBytes(UTF_8);
        SmileResponse<User> response = handler.handle(null, mockResponse(OK, MEDIA_TYPE_SMILE, invalidSmileBytes));

        assertFalse(response.hasValue());
        assertEquals(response.getException().getMessage(), format("Unable to create %s from SMILE response", User.class));
        assertTrue(response.getException().getCause() instanceof IllegalArgumentException);
        assertEquals(response.getException().getCause().getMessage(), "Invalid SMILE bytes for [simple type, class com.facebook.presto.server.smile.TestFullSmileResponseHandler$User]");

        assertEquals(response.getSmileBytes(), invalidSmileBytes);
        assertEquals(response.getResponseBytes(), response.getSmileBytes());
    }

    @Test
    public void testInvalidSmileGetValue()
    {
        byte[] invalidSmileBytes = "test".getBytes(UTF_8);
        SmileResponse<User> response = handler.handle(null, mockResponse(OK, MEDIA_TYPE_SMILE, invalidSmileBytes));

        try {
            response.getValue();
            fail("expected exception");
        }
        catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Response does not contain a SMILE value");
            assertEquals(e.getCause(), response.getException());

            assertEquals(response.getSmileBytes(), invalidSmileBytes);
            assertEquals(response.getResponseBytes(), response.getSmileBytes());
        }
    }

    @Test
    public void testNonSmileResponse()
    {
        SmileResponse<User> response = handler.handle(null, TestingResponse.mockResponse(OK, PLAIN_TEXT_UTF_8, "hello"));

        assertFalse(response.hasValue());
        assertNull(response.getException());
        assertNull(response.getSmileBytes());
        assertEquals(response.getResponseBytes(), "hello".getBytes(UTF_8));
    }

    @Test
    public void testMissingContentType()
    {
        SmileResponse<User> response = handler.handle(null,
                new TestingResponse(OK, ImmutableListMultimap.of(), "hello".getBytes(UTF_8)));

        assertFalse(response.hasValue());
        assertNull(response.getException());
        assertNull(response.getSmileBytes());
        assertEquals(response.getResponseBytes(), "hello".getBytes(UTF_8));
        assertTrue(response.getHeaders().isEmpty());
    }

    @Test
    public void testSmileErrorResponse()
    {
        User user = new User("Joe", 25);
        byte[] smileBytes = codec.toBytes(user);
        SmileResponse<User> response = handler.handle(null, mockResponse(INTERNAL_SERVER_ERROR, MEDIA_TYPE_SMILE, smileBytes));

        assertTrue(response.hasValue());
        assertEquals(response.getStatusCode(), INTERNAL_SERVER_ERROR.code());
        assertEquals(response.getResponseBytes(), response.getSmileBytes());
    }

    public static class User
    {
        private final String name;
        private final int age;

        @JsonCreator
        public User(@JsonProperty("name") String name, @JsonProperty("age") int age)
        {
            this.name = name;
            this.age = age;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public int getAge()
        {
            return age;
        }
    }

    private static Response mockResponse(HttpStatus status, MediaType type, byte[] content)
    {
        return new TestingResponse(status, contentType(type), content);
    }
}
