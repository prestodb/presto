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
package com.facebook.presto.verifier.framework;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Statement;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class QueryObjectBundle
        extends QueryBundle
{
    private final QualifiedName objectName;

    public QueryObjectBundle(
            QualifiedName objectName,
            List<Statement> setupQueries,
            Statement query,
            List<Statement> teardownQueries,
            ClusterType cluster)
    {
        super(setupQueries, query, teardownQueries, cluster);
        this.objectName = requireNonNull(objectName, "objectName is null");
    }

    public QualifiedName getObjectName()
    {
        return objectName;
    }
}
