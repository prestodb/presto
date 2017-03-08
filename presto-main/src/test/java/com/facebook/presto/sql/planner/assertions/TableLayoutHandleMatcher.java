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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.testing.TestingTransactionHandle;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class TableLayoutHandleMatcher
{
    private static final TableLayoutHandle ANY = new TableLayoutHandle(new ConnectorId("any"), TestingTransactionHandle.create(), new AnyConnectorTableLayoutHandle());
    private final TableLayoutHandle expectedTableLayout;

    public TableLayoutHandleMatcher(TableLayoutHandle expectedTableLayout)
    {
        this.expectedTableLayout = expectedTableLayout;
    }

    public static TableLayoutHandleMatcher any()
    {
        return new TableLayoutHandleMatcher(ANY);
    }

    public boolean matches(Optional<TableLayoutHandle> actualTableLayout)
    {
        return actualTableLayout.isPresent() && (expectedTableLayout == ANY || expectedTableLayout.equals(actualTableLayout));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("expectedTableLayout", expectedTableLayout)
                .toString();
    }

    private static class AnyConnectorTableLayoutHandle implements ConnectorTableLayoutHandle {}
}
