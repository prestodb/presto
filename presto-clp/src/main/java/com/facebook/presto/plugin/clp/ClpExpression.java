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
 * Represents the result of converting a Presto RowExpression into a CLP-compatible KQL query. In
 * every case, `pushDownExpression` represents the part of the RowExpression that could be
 * converted to a KQL expression, and `remainingExpression` represents the part that could not be
 * converted.
 */
public class ClpExpression
{
    // Optional KQL query or column name representing the fully or partially translatable part of the expression.
    private final Optional<String> pushDownExpression;

    // The remaining (non-translatable) portion of the RowExpression, if any.
    private final Optional<RowExpression> remainingExpression;

    public ClpExpression(String pushDownExpression, RowExpression remainingExpression)
    {
        this.pushDownExpression = Optional.ofNullable(pushDownExpression);
        this.remainingExpression = Optional.ofNullable(remainingExpression);
    }

    /**
     * Creates an empty ClpExpression with neither pushdown nor remaining expressions.
     */
    public ClpExpression()
    {
        this(null, null);
    }

    /**
     * Creates a ClpExpression from a fully translatable KQL query or column name.
     *
     * @param pushDownExpression
     */
    public ClpExpression(String pushDownExpression)
    {
        this(pushDownExpression, null);
    }

    /**
     * Creates a ClpExpression from a non-translatable RowExpression.
     *
     * @param remainingExpression
     */
    public ClpExpression(RowExpression remainingExpression)
    {
        this(null, remainingExpression);
    }

    public Optional<String> getPushDownExpression()
    {
        return pushDownExpression;
    }

    public Optional<RowExpression> getRemainingExpression()
    {
        return remainingExpression;
    }
}
