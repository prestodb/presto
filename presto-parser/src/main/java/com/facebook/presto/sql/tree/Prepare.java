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
package com.facebook.presto.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Prepare extends Statement
{
  private final String identifier;
  private final Query query;

  public Prepare(NodeLocation location, String identifier, Query query)
  {
    super(Optional.of(location));
    this.identifier = identifier;
    this.query = query;
  }

  public Prepare(String identifier, Query query)
  {
    super(Optional.empty());
    this.identifier = identifier;
    this.query = query;
  }

  public String getIdentifier()
  {
    return identifier;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context)
  {
    return visitor.visitPrepare(this, context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(identifier, query);
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
    Prepare o = (Prepare) obj;
    return Objects.equals(identifier, o.identifier) &&
            Objects.equals(query, o.query);
  }

  @Override
  public String toString()
  {
    return toStringHelper(this)
            .add("identifier", identifier)
            .add("query", query)
            .toString();
  }

  public Query getQuery()
  {
    return query;
  }
}
