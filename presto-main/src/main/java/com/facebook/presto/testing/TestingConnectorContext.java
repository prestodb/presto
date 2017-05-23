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
package com.facebook.presto.testing;

import com.facebook.presto.GroupByHashPageIndexerFactory;
import com.facebook.presto.MBeanNamespaceManager;
import com.facebook.presto.NamespacedMBeanServer;
import com.facebook.presto.PagesIndexPageSorter;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.connector.ConnectorAwareNodeManager;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PageSorter;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.type.TypeRegistry;

import javax.management.MBeanServer;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.MBeanNamespaceManager.createMBeanNamespaceManager;

public class TestingConnectorContext
        implements ConnectorContext
{
    private final NodeManager nodeManager = new ConnectorAwareNodeManager(new InMemoryNodeManager(), "testenv", new ConnectorId("test"));
    private final TypeManager typeManager = new TypeRegistry();
    private final MBeanNamespaceManager namespaceManager = createMBeanNamespaceManager();
    private final PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory());
    private final PageIndexerFactory pageIndexerFactory = new GroupByHashPageIndexerFactory(new JoinCompiler());
    private final NamespacedMBeanServer mBeanServer = namespaceManager.createMBeanServer("test" + ThreadLocalRandom.current().nextLong());

    public TestingConnectorContext()
    {
        // associate typeManager with a function registry
        new FunctionRegistry(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    }

    @Override
    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    @Override
    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    @Override
    public PageSorter getPageSorter()
    {
        return pageSorter;
    }

    @Override
    public PageIndexerFactory getPageIndexerFactory()
    {
        return pageIndexerFactory;
    }

    @Override
    public MBeanServer getMBeanServer()
    {
        return mBeanServer;
    }
}
