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
package com.facebook.presto.spi.security;

import com.facebook.presto.spi.predicate.SpiExpression;

import java.util.Objects;

public class RowLevelSecurityResponse
{
    private final SpiExpression spiExpression;
    private final String warning;

    public RowLevelSecurityResponse(SpiExpression spiExpression, String warning)
    {
        this.spiExpression = spiExpression;
        this.warning = warning;
    }

    public SpiExpression getSpiExpression()
    {
        return spiExpression;
    }

    public String getWarning()
    {
        return warning;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(spiExpression, warning);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowLevelSecurityResponse response = (RowLevelSecurityResponse) o;
        return Objects.equals(spiExpression, response.getSpiExpression()) &&
                Objects.equals(warning, response.getWarning());
    }
}
