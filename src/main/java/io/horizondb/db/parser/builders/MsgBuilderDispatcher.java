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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.BetweenPredicateContext;
import io.horizondb.db.parser.HqlParser.CreateDatabaseContext;
import io.horizondb.db.parser.HqlParser.CreateTimeSeriesContext;
import io.horizondb.db.parser.HqlParser.FieldDefinitionContext;
import io.horizondb.db.parser.HqlParser.FieldsDefinitionContext;
import io.horizondb.db.parser.HqlParser.InPredicateContext;
import io.horizondb.db.parser.HqlParser.InsertContext;
import io.horizondb.db.parser.HqlParser.PredicateContext;
import io.horizondb.db.parser.HqlParser.RecordDefinitionContext;
import io.horizondb.db.parser.HqlParser.RecordsDefinitionContext;
import io.horizondb.db.parser.HqlParser.SelectContext;
import io.horizondb.db.parser.HqlParser.SelectListContext;
import io.horizondb.db.parser.HqlParser.SimplePredicateContext;
import io.horizondb.db.parser.HqlParser.TimeSeriesOptionContext;
import io.horizondb.db.parser.HqlParser.UseDatabaseContext;
import io.horizondb.db.parser.HqlParser.WhereClauseContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;

import java.io.IOException;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>MsgBuilder</code> that dispatch to the proper builder based on the received callback.
 */
public final class MsgBuilderDispatcher extends HqlBaseListener implements MsgBuilder {

    /**
     * The database configuration.
     */
    private final Configuration configuration;
    
    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;
    
    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The name of the database on which the query must be executed.
     */
    private final String databaseName;
    
    /**
     * The builder to which the calls must be dispatched.
     */
    private MsgBuilder builder;
    
    /**
     * Creates a dispatcher.
     * 
     * @param configuration the database configuration
     * @param requestHeader the original request header
     * @param databaseName the name of the database on which the query must be executed
     */
    public MsgBuilderDispatcher(Configuration configuration, 
                                DatabaseManager databaseManager,
                                MsgHeader requestHeader, 
                                String databaseName) {
        
        this.configuration = configuration;
        this.databaseManager = databaseManager;
        this.requestHeader = requestHeader;
        this.databaseName = databaseName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterCreateDatabase(@NotNull CreateDatabaseContext ctx) {
        
        this.builder = new CreateDatabaseMsgBuilder(this.requestHeader);
        
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
        
        this.builder = new UseDatabaseMsgBuilder(this.requestHeader);
        this.builder.enterUseDatabase(ctx);
    }

    /**
     * {@inheritDoc}
     */    
    @Override
    public void enterCreateTimeSeries(@NotNull CreateTimeSeriesContext ctx) {
        
        this.builder = new CreateTimeSeriesMsgBuilder(this.configuration,
                                                      this.databaseManager,
                                                      this.requestHeader, 
                                                      this.databaseName);
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
        this.builder = new SelectMsgBuilder(this.databaseManager, this.requestHeader, this.databaseName);
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
    public void exitWhereClause(@NotNull WhereClauseContext ctx) {
        this.builder.exitWhereClause(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterWhereClause(@NotNull WhereClauseContext ctx) {
        this.builder.enterWhereClause(ctx);
    }

    /**    
     * {@inheritDoc}
     */    
    @Override
    public void enterPredicate(@NotNull PredicateContext ctx) {
        this.builder.enterPredicate(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void exitPredicate(@NotNull PredicateContext ctx) {
        this.builder.exitPredicate(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSimplePredicate(@NotNull SimplePredicateContext ctx) {
        this.builder.enterSimplePredicate(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterInPredicate(@NotNull InPredicateContext ctx) {
        this.builder.enterInPredicate(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterBetweenPredicate(@NotNull BetweenPredicateContext ctx) {
        this.builder.enterBetweenPredicate(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSelectList(@NotNull SelectListContext ctx) {
        this.builder.enterSelectList(ctx);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterInsert(@NotNull InsertContext ctx) {
        this.builder = new InsertMsgBuilder(this.databaseManager, this.requestHeader, this.databaseName);
        this.builder.enterInsert(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException, HorizonDBException {
        return this.builder.build();
    }
}
