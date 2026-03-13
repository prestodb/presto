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
package com.facebook.presto.hive.security;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

public class SecurityConfig
{
    private String securitySystem = "legacy";
    private boolean restrictProcedureCall = true;
    private String procedureAccessControlConfigFile;
    private Duration procedureAccessControlRefreshPeriod;

    @NotNull
    public String getSecuritySystem()
    {
        return securitySystem;
    }

    @Config("hive.security")
    public SecurityConfig setSecuritySystem(String securitySystem)
    {
        this.securitySystem = securitySystem;
        return this;
    }

    @Deprecated
    public boolean isRestrictProcedureCall()
    {
        return restrictProcedureCall;
    }

    @Deprecated
    @Config("hive.restrict-procedure-call")
    public SecurityConfig setRestrictProcedureCall(boolean restrictProcedureCall)
    {
        this.restrictProcedureCall = restrictProcedureCall;
        return this;
    }

    public String getProcedureAccessControlConfigFile()
    {
        return procedureAccessControlConfigFile;
    }

    @Config("hive.procedure-access-control.config-file")
    @ConfigDescription("Path to a JSON file containing procedure access control rules")
    public SecurityConfig setProcedureAccessControlConfigFile(String procedureAccessControlConfigFile)
    {
        this.procedureAccessControlConfigFile = procedureAccessControlConfigFile;
        return this;
    }

    @MinDuration("1ms")
    public Duration getProcedureAccessControlRefreshPeriod()
    {
        return procedureAccessControlRefreshPeriod;
    }

    @Config("hive.procedure-access-control.refresh-period")
    @ConfigDescription("How often the procedure access control rules file is reloaded")
    public SecurityConfig setProcedureAccessControlRefreshPeriod(Duration procedureAccessControlRefreshPeriod)
    {
        this.procedureAccessControlRefreshPeriod = procedureAccessControlRefreshPeriod;
        return this;
    }
}
