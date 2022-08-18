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
package com.facebook.presto.pinot.auth;

import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.auth.none.PinotEmptyAuthenticationProvider;
import com.facebook.presto.pinot.auth.password.PinotPasswordAuthenticationProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.pinot.PinotSessionProperties.BROKER_AUTHENTICATION_PASSWORD;
import static com.facebook.presto.pinot.PinotSessionProperties.BROKER_AUTHENTICATION_USER;
import static com.facebook.presto.pinot.PinotSessionProperties.CONTROLLER_AUTHENTICATION_PASSWORD;
import static com.facebook.presto.pinot.PinotSessionProperties.CONTROLLER_AUTHENTICATION_USER;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static org.testng.Assert.assertEquals;

public class TestPinotAuthenticationProvider
{
    @Test
    public void testPinotEmptyAuthenticationProvider()
    {
        PinotEmptyAuthenticationProvider pinotEmptyAuthenticationProvider = PinotEmptyAuthenticationProvider.instance();
        assertEquals(pinotEmptyAuthenticationProvider.getAuthenticationToken(), Optional.empty());
        assertEquals(pinotEmptyAuthenticationProvider.getAuthenticationToken(new TestingConnectorSession(ImmutableList.of(), ImmutableMap.of())), Optional.empty());
    }

    @Test
    public void testPinotPasswordAuthenticationProvider()
    {
        PinotPasswordAuthenticationProvider pinotPasswordAuthenticationProvider = new PinotPasswordAuthenticationProvider("admin", "verysecret", null, null);
        assertEquals(pinotPasswordAuthenticationProvider.getAuthenticationToken(), Optional.of("YWRtaW46dmVyeXNlY3JldA=="));
    }

    @Test
    public void testControllerAuth()
    {
        PinotConfig pinotConfig = new PinotConfig().setControllerAuthenticationType("PASSWORD").setControllerAuthenticationUser("admin").setControllerAuthenticationPassword("verysecret");
        PinotControllerAuthenticationProvider pinotControllerAuthenticationProvider = new PinotControllerAuthenticationProvider(pinotConfig);
        assertEquals(pinotControllerAuthenticationProvider.getAuthenticationToken(), Optional.of("YWRtaW46dmVyeXNlY3JldA=="));

        ConnectorSession connectorSession = new TestingConnectorSession(ImmutableList.of(stringProperty(CONTROLLER_AUTHENTICATION_USER, "Controller authentication user", pinotConfig.getControllerAuthenticationUser(), false), stringProperty(CONTROLLER_AUTHENTICATION_PASSWORD, "Controller authentication password", pinotConfig.getControllerAuthenticationPassword(), false)), ImmutableMap.of(CONTROLLER_AUTHENTICATION_PASSWORD, "secret"));
        assertEquals(pinotControllerAuthenticationProvider.getAuthenticationToken(connectorSession), Optional.of("YWRtaW46c2VjcmV0"));

        connectorSession = new TestingConnectorSession(ImmutableList.of(stringProperty(CONTROLLER_AUTHENTICATION_USER, "Controller authentication user", pinotConfig.getControllerAuthenticationUser(), false), stringProperty(CONTROLLER_AUTHENTICATION_PASSWORD, "Controller authentication password", pinotConfig.getControllerAuthenticationPassword(), false)), ImmutableMap.of(CONTROLLER_AUTHENTICATION_USER, "user", CONTROLLER_AUTHENTICATION_PASSWORD, "secret"));
        assertEquals(pinotControllerAuthenticationProvider.getAuthenticationToken(connectorSession), Optional.of("dXNlcjpzZWNyZXQ="));
    }

    @Test
    public void testBrokerAuth()
    {
        PinotConfig pinotConfig = new PinotConfig().setBrokerAuthenticationType("PASSWORD").setBrokerAuthenticationUser("admin").setBrokerAuthenticationPassword("verysecret");
        PinotBrokerAuthenticationProvider pinotBrokerAuthenticationProvider = new PinotBrokerAuthenticationProvider(pinotConfig);
        assertEquals(pinotBrokerAuthenticationProvider.getAuthenticationToken(), Optional.of("YWRtaW46dmVyeXNlY3JldA=="));

        ConnectorSession connectorSession = new TestingConnectorSession(ImmutableList.of(stringProperty(BROKER_AUTHENTICATION_USER, "Broker authentication user", pinotConfig.getBrokerAuthenticationUser(), false), stringProperty(BROKER_AUTHENTICATION_PASSWORD, "Broker authentication password", pinotConfig.getBrokerAuthenticationPassword(), false)), ImmutableMap.of(BROKER_AUTHENTICATION_PASSWORD, "secret"));
        assertEquals(pinotBrokerAuthenticationProvider.getAuthenticationToken(connectorSession), Optional.of("YWRtaW46c2VjcmV0"));

        connectorSession = new TestingConnectorSession(ImmutableList.of(stringProperty(BROKER_AUTHENTICATION_USER, "Broker authentication user", pinotConfig.getBrokerAuthenticationUser(), false), stringProperty(BROKER_AUTHENTICATION_PASSWORD, "Broker authentication password", pinotConfig.getBrokerAuthenticationPassword(), false)), ImmutableMap.of(BROKER_AUTHENTICATION_USER, "user", BROKER_AUTHENTICATION_PASSWORD, "secret"));
        assertEquals(pinotBrokerAuthenticationProvider.getAuthenticationToken(connectorSession), Optional.of("dXNlcjpzZWNyZXQ="));
    }
}
