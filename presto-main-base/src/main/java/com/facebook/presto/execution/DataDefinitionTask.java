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
package com.facebook.presto.execution;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface DataDefinitionTask<T extends Statement>
{
    String getName();

    default String explain(T statement, List<Expression> parameters)
    {
        if (statement instanceof Prepare) {
            return SqlFormatter.formatSql(statement, Optional.empty());
        }

        return SqlFormatter.formatSql(statement, Optional.of(parameters));
    }

    default void queryPermissionCheck(AccessControl accessControl, Identity identity, AccessControlContext context, String query, Map<String, String> preparedStatements, Map<QualifiedObjectName, ViewDefinition> viewDefinitions, Map<QualifiedObjectName, MaterializedViewDefinition> materializedViewDefinitions)
    {
        accessControl.checkQueryIntegrity(identity, context, query, preparedStatements, viewDefinitions, materializedViewDefinitions);
    }
}
