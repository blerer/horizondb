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

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.databases.Database;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.BetweenPredicateContext;
import io.horizondb.db.parser.HqlParser.InPredicateContext;
import io.horizondb.db.parser.HqlParser.PredicateContext;
import io.horizondb.db.parser.HqlParser.SelectContext;
import io.horizondb.db.parser.HqlParser.SelectListContext;
import io.horizondb.db.parser.HqlParser.SimplePredicateContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.db.series.TimeSeries;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.Projection;
import io.horizondb.model.core.predicates.Operator;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.SelectPayload;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>SelectQuery</code> message instances.
 */
final class SelectMsgBuilder extends HqlBaseListener implements MsgBuilder {

    /**
     * The database manager.
     */
    private final DatabaseManager databaseManager;

    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;

    /**
     * The name of the database in which the time series must be created.
     */
    private String databaseName; 

    /**
     * The time series name.
     */
    private String timeSeriesName;

    /**
     * The record and field projectionBuilder.
     */
    private ProjectionBuilder projectionBuilder;

    /**
     * The predicate builders.
     */
    private Deque<PredicateBuilder> predicateBuilders = new LinkedList<>();

    /**
     * Creates a new <code>CreateTimeSeriesRequestBuilder</code> instance.
     * 
     * @param requestHeader the original request header
     * @param database the database in which the time series must be created
     */
    public SelectMsgBuilder(DatabaseManager databaseManager, 
                            MsgHeader requestHeader, 
                            String databaseName) {

        this.databaseManager = databaseManager;
        this.requestHeader = requestHeader;
        this.databaseName = databaseName;
        this.predicateBuilders.addFirst(PredicateBuilders.noop());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSelect(@NotNull SelectContext ctx) {
        
        if (ctx.databaseName() != null) {
            this.databaseName = ctx.databaseName().getText();
        }
        this.timeSeriesName = ctx.ID().getText();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> build() throws IOException, HorizonDBException {

        Database database = this.databaseManager.getDatabase(this.databaseName);
        TimeSeries timeSeries = database.getTimeSeries(this.timeSeriesName);
        TimeSeriesDefinition definition = timeSeries.getDefinition();
        
        PredicateBuilder builder = this.predicateBuilders.poll();
        Predicate predicate = builder.build(definition);
        
        Projection projection = this.projectionBuilder.build(definition);
        SelectPayload payload = new SelectPayload(this.databaseName, 
                                                  this.timeSeriesName, 
                                                  projection, 
                                                  predicate);
        
        return Msg.newRequestMsg(this.requestHeader, OpCode.SELECT, payload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSelectList(@NotNull SelectListContext ctx) {
        this.projectionBuilder = new ProjectionBuilder(toList(ctx));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterPredicate(@NotNull PredicateContext ctx) {

           
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitPredicate(@NotNull PredicateContext ctx) {
        
        if (ctx.AND() != null) {
            
            PredicateBuilder right = this.predicateBuilders.removeFirst();
            PredicateBuilder left = this.predicateBuilders.removeFirst();
            
            PredicateBuilder expression = PredicateBuilders.and(left, right);
            
            this.predicateBuilders.addFirst(expression);
        
        } else if (ctx.OR() != null) {
            
            PredicateBuilder right = this.predicateBuilders.removeFirst();
            PredicateBuilder left = this.predicateBuilders.removeFirst();
            
            PredicateBuilder expression = PredicateBuilders.or(left, right);
            this.predicateBuilders.addFirst(expression);
        }
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterInPredicate(@NotNull InPredicateContext ctx) {
        
        int childCount = ctx.getChildCount();
        
        String fieldName = ctx.getChild(0).getText();
        
        boolean notIn = (ctx.NOT() != null);
        
        int start = 3;
        
        if (notIn) {
            
            start = 4;
        }
        
        List<String> values = new ArrayList<>();
        
        for (int i = start; i < childCount - 1; i += 2 ) {
            
            values.add(ctx.getChild(i).getText());
        }
        
        if (notIn) {
            
            this.predicateBuilders.addFirst(PredicateBuilders.notIn(fieldName, values));
            
        } else {
            
            this.predicateBuilders.addFirst(PredicateBuilders.in(fieldName, values));
        }
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterBetweenPredicate(@NotNull BetweenPredicateContext ctx) {

        String fieldName = ctx.getChild(0).getText();
        
        boolean notBetween = (ctx.NOT() != null);
        
        if (notBetween) {
            
            String min = ctx.getChild(3).getText();
            String max = ctx.getChild(5).getText();
            
            this.predicateBuilders.addFirst(PredicateBuilders.notBetween(fieldName, min, max));
        
        } else {
            
            String min = ctx.getChild(2).getText();
            String max = ctx.getChild(4).getText();
            
            this.predicateBuilders.addFirst(PredicateBuilders.between(fieldName, min, max));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSimplePredicate(@NotNull SimplePredicateContext ctx) {

      String fieldName = ctx.ID().getText();
      Operator operator = Operator.fromSymbol(ctx.operator().getText());
      String value = ctx.value().getText();
    
      this.predicateBuilders.addFirst(PredicateBuilders.simplePredicate(fieldName, operator, value));
    }
    
    /**
     * Extracts a list of values from the specified context. 
     * 
     * @param ctx the context from which the list must be extracted
     * @return a list of values
     */
    private static List<String> toList(ParserRuleContext ctx) {
        
        if (ctx == null) {
            return Collections.emptyList();
        }
        
        List<String> list = new ArrayList<>();
        
        for (int i  = 0, m = ctx.getChildCount(); i  < m; i += 2) {
            list.add(ctx.getChild(i).getText().trim());
        }
        
        return list;
    }
}
