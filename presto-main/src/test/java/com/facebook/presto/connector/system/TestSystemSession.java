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
package com.facebook.presto.connector.system;

import com.facebook.presto.Session;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestSystemSession
{
    @Test
    public void testSystemSessionForExtraCredentials()
    {
        Map<String, String> extraCredentials = ImmutableMap.of("key1", "value1", "key2", "value2");
        String user = "user";
        String principal = "principal";

        ConnectorTransactionHandle transactionHandle = new GlobalSystemTransactionHandle("connector-id", new TransactionId(UUID.randomUUID()));

        ConnectorIdentity connectorIdentity = new ConnectorIdentity(
                user,
                Optional.of(() -> principal),
                Optional.empty(),
                extraCredentials,
                ImmutableMap.of(),
                Optional.of("selectedUserName"),
                Optional.of("reasonForSelection"));

        TestingConnectorSession testingConnectorSession = new TestingConnectorSession(connectorIdentity,
                ImmutableList.of(new PropertyMetadata<>("property-name",
                        "This is a description",
                        VarcharType.createUnboundedVarcharType(),
                        String.class,
                        "defaultValue",
                        false,
                        Object::toString,
                        value -> value)));

        Session resultSession = SystemConnectorSessionUtil.toSession(transactionHandle, testingConnectorSession);

        assertNotNull(resultSession);
        assertEquals(resultSession.getIdentity().getUser(), "user");
        assertEquals(resultSession.getIdentity().getPrincipal().get().getName(), principal);
        assertEquals(resultSession.getIdentity().getExtraCredentials(), extraCredentials);
    }
}
