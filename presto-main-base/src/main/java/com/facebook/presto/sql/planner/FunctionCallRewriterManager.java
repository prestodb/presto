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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.function.FunctionCallRewriter;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Objects.requireNonNull;

/**
 * A general registry for FunctionCallRewriters.
 * Rewriters can register themselves to have their function call rewriting logic
 * applied during query optimization.
 */
public class FunctionCallRewriterManager
{
    private final Set<FunctionCallRewriter> rewriters = new CopyOnWriteArraySet<>();

    @Inject
    public FunctionCallRewriterManager() {}

    public void addRewriter(FunctionCallRewriter rewriter)
    {
        requireNonNull(rewriter, "rewriter is null");
        rewriters.add(rewriter);
    }

    public void removeRewriter(FunctionCallRewriter rewriter)
    {
        requireNonNull(rewriter, "rewriter is null");
        rewriters.remove(rewriter);
    }

    public Set<FunctionCallRewriter> getAllRewriters()
    {
        return ImmutableSet.copyOf(rewriters);
    }
}
