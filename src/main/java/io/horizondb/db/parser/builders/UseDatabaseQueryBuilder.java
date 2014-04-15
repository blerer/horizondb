/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.db.parser.builders;

import io.horizondb.db.Query;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.UseDatabaseContext;
import io.horizondb.db.parser.QueryBuilder;
import io.horizondb.db.queries.UseDatabaseQuery;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>UseDatabaseQuery</code> instances.
 * 
 * @author Benjamin
 *
 */
final class UseDatabaseQueryBuilder extends HqlBaseListener implements QueryBuilder {

    /**
     * The name of the database that must be used.
     */
    private String databaseName;

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterUseDatabase(@NotNull UseDatabaseContext ctx) {
        this.databaseName = ctx.ID().getText();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query build() {

        return new UseDatabaseQuery(this.databaseName);
    }

}
