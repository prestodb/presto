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

import com.facebook.presto.spi.relation.RowExpression;

import static java.util.Objects.requireNonNull;

public class ConnectorPushdownFilterResult
{
    private final ConnectorTableLayout layout;
    private final RowExpression unenforcedConstraint;

    public ConnectorPushdownFilterResult(ConnectorTableLayout layout, RowExpression unenforcedConstraint)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
    }

    public ConnectorTableLayout getLayout()
    {
        return layout;
    }

    public RowExpression getUnenforcedConstraint()
    {
        return unenforcedConstraint;
    }
}
