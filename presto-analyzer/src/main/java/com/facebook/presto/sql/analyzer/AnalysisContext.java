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
package com.facebook.presto.sql.analyzer;

/**
 * This class should contain all the necessary information required for analysis. The analysis phase should not depend on the session or any other configuration.
 */
public final class AnalysisContext
{
    //TODO: These should move moved from here after accesscontrol is removed from analysis phase.
    private boolean checkAccessControlWithSubfields;
    private boolean checkAccessControlOnUtilizedColumnsOnly;

    public boolean isCheckAccessControlWithSubfields()
    {
        return checkAccessControlWithSubfields;
    }

    public AnalysisContext setCheckAccessControlWithSubfields(boolean checkAccessControlWithSubfields)
    {
        this.checkAccessControlWithSubfields = checkAccessControlWithSubfields;
        return this;
    }

    public boolean isCheckAccessControlOnUtilizedColumnsOnly()
    {
        return checkAccessControlOnUtilizedColumnsOnly;
    }

    public AnalysisContext setCheckAccessControlOnUtilizedColumnsOnly(boolean checkAccessControlOnUtilizedColumnsOnly)
    {
        this.checkAccessControlOnUtilizedColumnsOnly = checkAccessControlOnUtilizedColumnsOnly;
        return this;
    }
}
