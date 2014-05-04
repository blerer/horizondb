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
import io.horizondb.db.parser.HqlParser.BetweenExpressionContext;
import io.horizondb.db.parser.HqlParser.CreateDatabaseContext;
import io.horizondb.db.parser.HqlParser.CreateTimeSeriesContext;
import io.horizondb.db.parser.HqlParser.ExpressionContext;
import io.horizondb.db.parser.HqlParser.FieldDefinitionContext;
import io.horizondb.db.parser.HqlParser.FieldsDefinitionContext;
import io.horizondb.db.parser.HqlParser.InExpressionContext;
import io.horizondb.db.parser.HqlParser.RecordDefinitionContext;
import io.horizondb.db.parser.HqlParser.RecordsDefinitionContext;
import io.horizondb.db.parser.HqlParser.SelectContext;
import io.horizondb.db.parser.HqlParser.SimpleExpressionContext;
import io.horizondb.db.parser.HqlParser.TimeSeriesOptionContext;
import io.horizondb.db.parser.HqlParser.UseDatabaseContext;
import io.horizondb.db.parser.HqlParser.WhereDefinitionContext;
import io.horizondb.db.parser.QueryBuilder;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>QueryBuilder</code> that dispatch to the prober builder based on the received callback.
 * 
 * @author Benjamin
 *
 */
public final class QueryBuilderDispatcher extends HqlBaseListener implements QueryBuilder {

    /**
     * The builder to which the calls must be dispatched.
     */
    private QueryBuilder builder;

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterCreateDatabase(@NotNull CreateDatabaseContext ctx) {
        
        this.builder = new CreateDatabaseQueryBuilder();
        
        this.builder.enterCreateDatabase(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void exitUseDatabase(@NotNull UseDatabaseContext ctx) {
        this.builder.exitUseDatabase(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void exitCreateDatabase(@NotNull CreateDatabaseContext ctx) {
        this.builder.exitCreateDatabase(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterFieldsDefinition(@NotNull FieldsDefinitionContext ctx) {
        this.builder.enterFieldsDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitFieldsDefinition(@NotNull FieldsDefinitionContext ctx) {
        this.builder.exitFieldsDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterRecordDefinition(@NotNull RecordDefinitionContext ctx) {
        this.builder.enterRecordDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitRecordDefinition(@NotNull RecordDefinitionContext ctx) {
        this.builder.exitRecordDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterFieldDefinition(@NotNull FieldDefinitionContext ctx) {
        this.builder.enterFieldDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitFieldDefinition(@NotNull FieldDefinitionContext ctx) {
        this.builder.exitFieldDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterRecordsDefinition(@NotNull RecordsDefinitionContext ctx) {
        this.builder.enterRecordsDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitRecordsDefinition(@NotNull RecordsDefinitionContext ctx) {
        this.builder.exitRecordsDefinition(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitCreateTimeSeries(@NotNull CreateTimeSeriesContext ctx) {
        this.builder.exitCreateTimeSeries(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterUseDatabase(@NotNull UseDatabaseContext ctx) {
        
        this.builder = new UseDatabaseQueryBuilder();
        this.builder.enterUseDatabase(ctx);
    }

    /**
     * {@inheritDoc}
     */    
    @Override
    public void enterCreateTimeSeries(@NotNull CreateTimeSeriesContext ctx) {
        this.builder = new CreateTimeSeriesQueryBuilder();
        this.builder.enterCreateTimeSeries(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterTimeSeriesOption(@NotNull TimeSeriesOptionContext ctx) {
        this.builder.enterTimeSeriesOption(ctx);
    }
    
    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterSelect(@NotNull SelectContext ctx) {
        this.builder = new SelectQueryBuilder();
        this.builder.enterSelect(ctx);
    }

    /**    
     * {@inheritDoc}
     */    
    @Override
    public void exitSelect(@NotNull SelectContext ctx) {
        this.builder.exitSelect(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void exitWhereDefinition(@NotNull WhereDefinitionContext ctx) {
        this.builder.exitWhereDefinition(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterWhereDefinition(@NotNull WhereDefinitionContext ctx) {
        this.builder.enterWhereDefinition(ctx);
    }

    /**    
     * {@inheritDoc}
     */    
    @Override
    public void enterExpression(@NotNull ExpressionContext ctx) {
        this.builder.enterExpression(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void exitExpression(@NotNull ExpressionContext ctx) {
        this.builder.exitExpression(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSimpleExpression(@NotNull SimpleExpressionContext ctx) {
        this.builder.enterSimpleExpression(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterInExpression(@NotNull InExpressionContext ctx) {
        this.builder.enterInExpression(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterBetweenExpression(@NotNull BetweenExpressionContext ctx) {
        this.builder.enterBetweenExpression(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query build() {
        return this.builder.build();
    }
}
