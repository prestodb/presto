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
package com.facebook.presto.kafka;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.AssertTrue;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.testing.ValidationAssertions.assertFailsValidation;
import static com.facebook.airlift.testing.ValidationAssertions.assertValidates;
import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

public class TestKafkaSecurityConfig
{
    private static final String INVALID_ERROR_MESSAGE = "Only SASL_PLAINTEXT and SASL_SSL security protocols are supported. See 'kafka.config.resources' if other security protocols are needed";
    private static final String IS_VALID_SECURITY_PROTOCOL = "validSecurityProtocol";

    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KafkaSecurityConfig.class)
                .setSecurityProtocol(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.security-protocol", SASL_SSL.name())
                .build();

        KafkaSecurityConfig expected = new KafkaSecurityConfig().setSecurityProtocol(SASL_SSL);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidSecurityProtocols()
    {
        assertValidates(new KafkaSecurityConfig()
                .setSecurityProtocol(SASL_PLAINTEXT));

        assertValidates(new KafkaSecurityConfig()
                .setSecurityProtocol(SASL_SSL));
    }

    @Test
    public void testInvalidSecurityProtocol()
    {
        assertFailsValidation(new KafkaSecurityConfig().setSecurityProtocol(PLAINTEXT),
                IS_VALID_SECURITY_PROTOCOL,
                INVALID_ERROR_MESSAGE,
                AssertTrue.class);

        assertFailsValidation(new KafkaSecurityConfig().setSecurityProtocol(SSL),
                IS_VALID_SECURITY_PROTOCOL,
                INVALID_ERROR_MESSAGE,
                AssertTrue.class);
    }
}
