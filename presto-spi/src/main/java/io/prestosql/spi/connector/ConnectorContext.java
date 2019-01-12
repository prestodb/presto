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
package io.prestosql.spi.connector;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.type.TypeManager;

public interface ConnectorContext
{
    default NodeManager getNodeManager()
    {
        throw new UnsupportedOperationException();
    }

    default TypeManager getTypeManager()
    {
        throw new UnsupportedOperationException();
    }

    default PageSorter getPageSorter()
    {
        throw new UnsupportedOperationException();
    }

    default PageIndexerFactory getPageIndexerFactory()
    {
        throw new UnsupportedOperationException();
    }
}
