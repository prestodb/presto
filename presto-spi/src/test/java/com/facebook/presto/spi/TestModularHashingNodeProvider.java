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
package com.facebook.presto.spi;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestModularHashingNodeProvider
{
    @Test
    public void testEquality()
    {
        TestingProviderNode node1 = new TestingProviderNode(10);
        TestingProviderNode node2 = new TestingProviderNode(20);
        TestingProviderNode node3 = new TestingProviderNode(30);
        TestingProviderNode node4 = new TestingProviderNode(40);
        NodeProvider<TestingProviderNode> nodeProvider = new ModularHashingNodeProvider<>(ImmutableList.of(node1, node2, node3, node4));
        assertEquals(nodeProvider.get(13, 2), ImmutableList.of(node2, node3));
        assertEquals(nodeProvider.get(23, 5), ImmutableList.of(node4, node1, node2, node3));
    }
}
