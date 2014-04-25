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
import io.horizondb.db.parser.HqlParser.BooleanExpressionContext;
import io.horizondb.db.parser.HqlParser.SelectContext;
import io.horizondb.db.parser.QueryBuilder;
import io.horizondb.db.queries.Criteria;
import io.horizondb.db.queries.Criterion;
import io.horizondb.db.queries.Operator;
import io.horizondb.db.queries.SelectQuery;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>SelectQuery</code> instances.
 * 
 * @author Benjamin
 *
 */
final class SelectQueryBuilder extends HqlBaseListener implements QueryBuilder {
    
    /**
     * The time series name.
     */
    private String timeSeriesName;
    
    /**
     * The builder used to create the <code>Criteria</code>.
     */
    private Criteria.Builder builder = Criteria.newBuilder();

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSelect(@NotNull SelectContext ctx) {
        this.timeSeriesName = ctx.ID().getText();
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterBooleanExpression(@NotNull BooleanExpressionContext ctx) {
        
        String fieldName = ctx.ID().getText();
        Operator operator = Operator.fromSymbol(ctx.operator().getText());
        String value = ctx.value().getText();
        
        this.builder.add(new Criterion(fieldName, operator, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query build() {

        return new SelectQuery(this.timeSeriesName, this.builder.build());
    }
}
