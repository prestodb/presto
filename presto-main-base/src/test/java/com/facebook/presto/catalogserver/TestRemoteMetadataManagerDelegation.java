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
package com.facebook.presto.catalogserver;

import com.facebook.presto.metadata.DelegatingMetadataManager;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Verifies that RemoteMetadataManager does not override DDL methods that must be forwarded to
 * the underlying MetadataManager (i.e., to the connector). Methods that are overridden with an
 * empty body in RemoteMetadataManager silently swallow DDL on catalog-server-enabled coordinators.
 *
 * <p>The correct pattern: DDL methods NOT listed in the class's own declared methods fall through
 * to DelegatingMetadataManager, which delegates to MetadataManager → connector.
 */
public class TestRemoteMetadataManagerDelegation
{
    /**
     * setFieldType must NOT be overridden in RemoteMetadataManager.
     * Removing the override causes the call to fall through to
     * DelegatingMetadataManager.setFieldType() → delegate.setFieldType() = MetadataManager → connector.
     *
     * <p>If this test fails it means someone re-introduced an empty override stub.
     */
    @Test
    public void testSetFieldTypeNotOverriddenInRemoteMetadataManager()
    {
        List<String> declaredMethodNames = Arrays.stream(RemoteMetadataManager.class.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toList());

        assertFalse(
                declaredMethodNames.contains("setFieldType"),
                "RemoteMetadataManager must not override setFieldType — an empty stub silently swallows " +
                        "nested field type DDL on catalog-server-enabled coordinators. " +
                        "The call must fall through to DelegatingMetadataManager.setFieldType().");
    }

    /**
     * DelegatingMetadataManager must override setFieldType, guaranteeing that the delegation
     * chain RemoteMetadataManager → DelegatingMetadataManager → MetadataManager is complete.
     */
    @Test
    public void testSetFieldTypePresentInDelegatingMetadataManager()
    {
        List<String> declaredMethodNames = Arrays.stream(DelegatingMetadataManager.class.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toList());

        assertTrue(
                declaredMethodNames.contains("setFieldType"),
                "DelegatingMetadataManager must override setFieldType so that the delegation chain " +
                        "from RemoteMetadataManager reaches MetadataManager.");
    }
}
