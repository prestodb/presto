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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.RevokeRoles;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.createPrincipal;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ROLE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class RevokeRolesTask
        implements DataDefinitionTask<RevokeRoles>
{
    @Override
    public String getName()
    {
        return "GRANT ROLE";
    }

    @Override
    public ListenableFuture<?> execute(RevokeRoles statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        Set<String> roles = statement.getRoles().stream().map(role -> role.getValue().toLowerCase(Locale.ENGLISH)).collect(toImmutableSet());
        Set<PrestoPrincipal> grantees = statement.getGrantees().stream()
                .map(MetadataUtil::createPrincipal)
                .collect(toImmutableSet());
        boolean adminOptionFor = statement.isAdminOptionFor();
        Optional<PrestoPrincipal> grantor = statement.getGrantor().map(specification -> createPrincipal(session, specification));
        String catalog = createCatalogName(session, statement);

        Set<String> availableRoles = metadata.listRoles(session, catalog);
        Set<String> specifiedRoles = new LinkedHashSet<>();
        specifiedRoles.addAll(roles);
        grantees.stream()
                .filter(principal -> principal.getType() == ROLE)
                .map(PrestoPrincipal::getName)
                .forEach(specifiedRoles::add);
        if (grantor.isPresent() && grantor.get().getType() == ROLE) {
            specifiedRoles.add(grantor.get().getName());
        }

        for (String role : specifiedRoles) {
            if (!availableRoles.contains(role)) {
                throw new SemanticException(MISSING_ROLE, statement, "Role '%s' does not exist", role);
            }
        }

        accessControl.checkCanRevokeRoles(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), roles, grantees, adminOptionFor, grantor, catalog);
        metadata.revokeRoles(session, roles, grantees, adminOptionFor, grantor, catalog);

        return immediateFuture(null);
    }
}
