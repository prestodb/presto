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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Explain
        extends Statement
{
    private final Statement statement;
    private final boolean analyze;
    private final boolean verbose;
    private final List<ExplainOption> options;

    public Explain(Statement statement, boolean analyze, boolean verbose, List<ExplainOption> options)
    {
        this(Optional.empty(), analyze, verbose, statement, options);
    }

    public Explain(NodeLocation location, boolean analyze, boolean verbose, Statement statement, List<ExplainOption> options)
    {
        this(Optional.of(location), analyze, verbose, statement, options);
    }

    private Explain(Optional<NodeLocation> location, boolean analyze, boolean verbose, Statement statement, List<ExplainOption> options)
    {
        super(location);
        this.statement = requireNonNull(statement, "statement is null");
        this.analyze = analyze;
        this.verbose = verbose;
        if (options == null) {
            this.options = ImmutableList.of();
        }
        else {
            this.options = ImmutableList.copyOf(options);
        }
    }

    public Statement getStatement()
    {
        return statement;
    }

    public boolean isAnalyze()
    {
        return analyze;
    }

    public boolean isVerbose()
    {
        return verbose;
    }

    public List<ExplainOption> getOptions()
    {
        return options;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExplain(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(statement)
                .addAll(options)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statement, options, analyze);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Explain o = (Explain) obj;
        return Objects.equals(statement, o.statement) &&
                Objects.equals(options, o.options) &&
                Objects.equals(analyze, o.analyze);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statement", statement)
                .add("options", options)
                .add("analyze", analyze)
                .toString();
    }
}
