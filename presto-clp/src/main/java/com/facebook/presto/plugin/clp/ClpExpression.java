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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.relation.RowExpression;

import java.util.Optional;

/**
 * Represents the result of:
 * <ul>
 *     <li>splitting a {@link RowExpression} into an expression that can be pushed down to CLP
 *     (`pushDownExpression`) and any remaining expression that cannot be
 *     (`remainingExpression`).</li>
 *     <li>a SQL query for filtering splits using CLP's metadata database.</li>
 * </ul>
 */
public class ClpExpression
{
    // Optional KQL query or column name representing the fully or partially translatable part of the expression.
    private final Optional<String> pushDownExpression;

    // Optional SQL string extracted from the query plan, which is only made of up of columns in
    // CLP's metadata database.
    private final Optional<String> metadataSqlQuery;

    // The remaining (non-translatable) portion of the RowExpression, if any.
    private final Optional<RowExpression> remainingExpression;

    public ClpExpression(String pushDownExpression, String metadataSqlQuery, RowExpression remainingExpression)
    {
        this.pushDownExpression = Optional.ofNullable(pushDownExpression);
        this.metadataSqlQuery = Optional.ofNullable(metadataSqlQuery);
        this.remainingExpression = Optional.ofNullable(remainingExpression);
    }

    /**
     * Creates an empty ClpExpression with neither pushdown nor remaining expressions.
     */
    public ClpExpression()
    {
        this(null, null, null);
    }

    /**
     * Creates a ClpExpression from a fully translatable KQL query or column name.
     *
     * @param pushDownExpression
     */
    public ClpExpression(String pushDownExpression)
    {
        this(pushDownExpression, null, null);
    }

    /**
     * Creates a ClpExpression from a fully translatable KQL string or column name, as well as a
     * metadata SQL string.
     *
     * @param pushDownExpression
     * @param metadataSqlQuery
     */
    public ClpExpression(String pushDownExpression, String metadataSqlQuery)
    {
        this(pushDownExpression, metadataSqlQuery, null);
    }

    /**
     * Creates a ClpExpression from a non-translatable RowExpression.
     *
     * @param remainingExpression
     */
    public ClpExpression(RowExpression remainingExpression)
    {
        this(null, null, remainingExpression);
    }

    public Optional<String> getPushDownExpression()
    {
        return pushDownExpression;
    }

    public Optional<String> getMetadataSqlQuery()
    {
        return metadataSqlQuery;
    }

    public Optional<RowExpression> getRemainingExpression()
    {
        return remainingExpression;
    }
}
