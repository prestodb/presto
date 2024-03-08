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
package com.facebook.presto.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class CTEInformation
{
    // Name of the common table expression as used in the query
    private final String cteName;
    //number of references of the CTE in the query
    private int numberOfReferences;
    private final boolean isView;
    private final boolean isMaterialized;
    private final String cteId;

    @JsonCreator
    public CTEInformation(
            @JsonProperty("cteName") String cteName,
            @JsonProperty("cteId") String cteId,
            @JsonProperty("numberOfReferences") int numberOfReferences,
            @JsonProperty("isView") boolean isView,
            @JsonProperty("isMaterialized") boolean isMaterialized)
    {
        this.cteName = requireNonNull(cteName, "cteName is null");
        this.numberOfReferences = numberOfReferences;
        this.isView = isView;
        this.isMaterialized = isMaterialized;
        this.cteId = cteId;
    }

    @JsonProperty
    public String getCteName()
    {
        return cteName;
    }

    @JsonProperty
    public boolean isMaterialized()
    {
        return isMaterialized;
    }

    @JsonProperty
    public int getNumberOfReferences()
    {
        return numberOfReferences;
    }

    @JsonProperty
    public String getCteId()
    {
        return cteId;
    }

    @JsonProperty
    public boolean getIsView()
    {
        return isView;
    }

    public void incrementReferences()
    {
        numberOfReferences++;
    }
}
