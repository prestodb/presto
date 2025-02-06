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
package com.facebook.presto.rewriter.optplus;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.rewriter.QueryRewriter;
import com.facebook.presto.spi.rewriter.QueryRewriterInput;
import com.facebook.presto.spi.rewriter.QueryRewriterOutput;
import com.facebook.presto.spi.rewriter.QueryRewriterProvider;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.Principal;
import java.util.List;

import static com.facebook.presto.rewriter.password.PluginPasswordAuthenticator.ADMINISTRATOR_ROLE;

public final class PassThroughQueryRewriter
        implements QueryRewriterProvider, QueryRewriter
{
    private final ExecuteOptimizerPlus executeOptimizerPlus = new ExecuteOptimizerPlus();
    private final OptplusQueryRewriter optplusQueryRewriter;
    private static final Logger log = Logger.get(PassThroughQueryRewriter.class);
    private boolean byPassAdminCheck;

    @Inject
    public PassThroughQueryRewriter(OptplusQueryRewriter optplusQueryRewriter)
    {
        this.optplusQueryRewriter = optplusQueryRewriter;
    }

    @Override
    public QueryRewriterOutput rewriteSQL(QueryRewriterInput queryRewriterInput)
    {
        String query = queryRewriterInput.getQuery().trim();
        if (query.contains("ExecuteWxdQueryOptimizer")) {
            if (!isAdmin(queryRewriterInput.getIdentity())) {
                throw new AccessDeniedException("A user must be an admin to run ExecuteWxdQueryOptimizer");
            }
            String rewrittenQuery;
            try {
                List<List<Object>> queryResult = executeOptimizerPlus.callExecuteOptimizerPlus(queryRewriterInput.getQuery());
                rewrittenQuery = RewriterUtils.generateValuesQuery(queryResult, executeOptimizerPlus.getColumns());
            }
            catch (Exception e) {
                throw new PrestoException(OptPlusErrorCode.PASS_THROUGH_ERROR_CODE, e);
            }
            return new QueryRewriterOutput.Builder()
                    .setRewrittenQuery(rewrittenQuery)
                    .setOriginalQuery(query)
                    .setQueryId(queryRewriterInput.getQueryId()).build();
        }
        else {
            return optplusQueryRewriter.rewriteSQL(queryRewriterInput);
        }
    }

    @VisibleForTesting
    void setByPassAdminCheck(boolean byPassAdminCheck)
    {
        this.byPassAdminCheck = byPassAdminCheck;
    }

    private boolean isAdmin(Identity identity)
    {
        if (byPassAdminCheck) {
            log.warn("Bypassing admin check in testing mode.");
            return true;
        }
        if (identity == null || !identity.getPrincipal().isPresent()) {
            return false;
        }
        Principal userPrincipal = identity.getPrincipal().get();
        try {
            MethodHandle getRole = MethodHandles.lookup().findVirtual(userPrincipal.getClass(), "getRole", MethodType.methodType(String.class));
            return ADMINISTRATOR_ROLE.equals(getRole.invoke(userPrincipal));
        }
        catch (Throwable e) {
            log.error(e, "Error getting role from principal %s", userPrincipal.getName());
        }
        return false;
    }

    @Override
    public QueryRewriter getQueryRewriter()
    {
        return this;
    }
}
