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
package io.prestosql.connector;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.type.TypeManager;

import static java.util.Objects.requireNonNull;

public class ConnectorContextInstance
        implements ConnectorContext
{
    private final NodeManager nodeManager;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final PageIndexerFactory pageIndexerFactory;

    public ConnectorContextInstance(
            NodeManager nodeManager,
            TypeManager typeManager,
            PageSorter pageSorter,
            PageIndexerFactory pageIndexerFactory)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
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
}
