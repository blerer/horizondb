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
import io.horizondb.db.parser.HqlParser.CreateDatabaseContext;
import io.horizondb.db.parser.QueryBuilder;
import io.horizondb.db.queries.CreateDatabaseQuery;
import io.horizondb.model.schema.DatabaseDefinition;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>CreateDatabaseQuery</code> instances.
 * 
 * @author Benjamin
 *
 */
final class CreateDatabaseQueryBuilder extends HqlBaseListener implements QueryBuilder {

    /**
     * The definition of the database that must be created.
     */
    private DatabaseDefinition definition;
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterCreateDatabase(@NotNull CreateDatabaseContext ctx) {

        String databaseName = ctx.ID().getText();
        this.definition = new DatabaseDefinition(databaseName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query build() {

        return new CreateDatabaseQuery(this.definition);
    }

}
