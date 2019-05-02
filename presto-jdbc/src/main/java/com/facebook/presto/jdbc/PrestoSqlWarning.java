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
package com.facebook.presto.jdbc;

import com.facebook.presto.spi.PrestoWarning;

import java.sql.SQLWarning;

public class PrestoSqlWarning
        extends SQLWarning
{
    public PrestoSqlWarning()
    {
        super();
    }

    public PrestoSqlWarning(PrestoWarning warning)
    {
        //TODO: enforce that sqlState is 01[5,6,7,8,9,I-Z][0-9A-Z]{3}
        // From the SQL Standard ISO_IEC_9075-2E_2016 24.1 SQLState: warning codes have class 01
        // warning subclasses defined in the sql state will start with [0-4A-H] and contain 3 characters
        // An example of vendor specific warning codes can be found here:
        // https://www.ibm.com/support/knowledgecenter/en/SSEPEK_10.0.0/codes/src/tpc/db2z_sqlstatevalues.html#db2z_sqlstatevalues__code01
        // Note that the subclass begins with 5 which indicates it is an implementation defined subclass
        super(warning.getMessage(), warning.getWarningCode().getName(), warning.getWarningCode().getCode());
    }
}
