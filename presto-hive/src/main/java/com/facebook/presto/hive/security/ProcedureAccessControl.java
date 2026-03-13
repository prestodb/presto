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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.plugin.base.security.AccessControlRules;
import com.facebook.presto.plugin.base.security.ProcedureAccessControlRule;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;
import static com.facebook.presto.plugin.base.security.ProcedureAccessControlRule.ProcedurePrivilege.EXECUTE;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCallProcedure;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ProcedureAccessControl
{
    private static final Logger log = Logger.get(ProcedureAccessControl.class);

    private final Supplier<List<ProcedureAccessControlRule>> procedureRules;

    public ProcedureAccessControl(SecurityConfig securityConfig)
    {
        requireNonNull(securityConfig, "securityConfig is null");

        String configFile = securityConfig.getProcedureAccessControlConfigFile();
        Duration refreshPeriod = securityConfig.getProcedureAccessControlRefreshPeriod();

        checkArgument(configFile != null || refreshPeriod == null,
                "hive.procedure-access-control.refresh-period requires hive.procedure-access-control.config-file to be set");

        if (configFile == null) {
            procedureRules = ImmutableList::of;
            return;
        }

        if (refreshPeriod != null) {
            procedureRules = memoizeWithExpiration(
                    () -> {
                        log.info("Refreshing Hive procedure access control rules from %s", configFile);
                        return loadProcedureRules(configFile);
                    },
                    refreshPeriod.toMillis(),
                    MILLISECONDS);
        }
        else {
            List<ProcedureAccessControlRule> loadedRules = loadProcedureRules(configFile);
            procedureRules = () -> loadedRules;
        }
    }

    public void checkCanCallProcedure(ConnectorIdentity identity, SchemaTableName procedureName)
    {
        if (!checkProcedurePermission(identity, procedureName, EXECUTE)) {
            denyCallProcedure(procedureName.toString());
        }
    }

    private boolean checkProcedurePermission(ConnectorIdentity identity, SchemaTableName procedureName, ProcedureAccessControlRule.ProcedurePrivilege... requiredPrivileges)
    {
        for (ProcedureAccessControlRule rule : procedureRules.get()) {
            Optional<Set<ProcedureAccessControlRule.ProcedurePrivilege>> procedurePrivileges = rule.match(identity.getUser(), procedureName);
            if (procedurePrivileges.isPresent()) {
                return procedurePrivileges.get().containsAll(ImmutableSet.copyOf(requiredPrivileges));
            }
        }
        return false;
    }

    private static List<ProcedureAccessControlRule> loadProcedureRules(String configFile)
    {
        AccessControlRules rules = parseJson(Paths.get(configFile), AccessControlRules.class);
        return rules.getProcedureRules();
    }
}

