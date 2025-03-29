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
package com.facebook.presto.spi.plan;

/*
 * Contains information about the identifier (cteId) of the CTE being materialized. This information is stored tablescans and tablefinish plan nodesg
 */
public class CteMaterializationInfo
{
    private final String cteId;

    public CteMaterializationInfo(String cteId)
    {
        this.cteId = cteId;
    }

    public String getCteId()
    {
        return cteId;
    }
}
