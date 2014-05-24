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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ConnectorSession;
import org.intellij.lang.annotations.Language;

import java.io.Closeable;
import java.util.List;

public interface QueryRunner
        extends Closeable
{
    @Override
    void close();

    int getNodeCount();

    MaterializedResult execute(@Language("SQL") String sql);

    MaterializedResult execute(ConnectorSession session, @Language("SQL") String sql);

    List<QualifiedTableName> listTables(ConnectorSession session, String catalog, String schema);

    boolean tableExists(ConnectorSession session, String table);
}
