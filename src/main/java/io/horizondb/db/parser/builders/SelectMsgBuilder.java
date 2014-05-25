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

import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.BetweenExpressionContext;
import io.horizondb.db.parser.HqlParser.ExpressionContext;
import io.horizondb.db.parser.HqlParser.InExpressionContext;
import io.horizondb.db.parser.HqlParser.SelectContext;
import io.horizondb.db.parser.HqlParser.SimpleExpressionContext;
import io.horizondb.db.parser.MsgBuilder;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.predicates.Operator;
import io.horizondb.model.core.predicates.Predicates;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.SelectPayload;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>SelectQuery</code> message instances.
 * 
 * @author Benjamin
 *
 */
final class SelectMsgBuilder extends HqlBaseListener implements MsgBuilder {
    
    /**
     * The original request header.
     */
    private final MsgHeader requestHeader;
    
    /**
     * The name of the database in which the time series must be created.
     */
    private final String database; 
    
    /**
     * The time series name.
     */
    private String timeSeriesName;
    
    /**
     * The predicates.
     */
    private Deque<Predicate> predicates = new LinkedList<>();
        
    /**
     * Creates a new <code>CreateTimeSeriesRequestBuilder</code> instance.
     * 
     * @param requestHeader the original request header
     * @param database the database in which the time series must be created
     */
    public SelectMsgBuilder(MsgHeader requestHeader, String database) {
        
        this.requestHeader = requestHeader;
        this.database = database;
        this.predicates.addFirst(Predicates.noop());
    }

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
    public Msg<?> build() {

        SelectPayload payload = new SelectPayload(this.database, this.timeSeriesName, this.predicates.poll());
        return Msg.newRequestMsg(this.requestHeader, OpCode.SELECT, payload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterExpression(@NotNull ExpressionContext ctx) {

           
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exitExpression(@NotNull ExpressionContext ctx) {
        
        if (ctx.AND() != null) {
            
            Predicate right = this.predicates.removeFirst();
            Predicate left = this.predicates.removeFirst();
            
            Predicate expression = Predicates.and(left, right);
            
            this.predicates.addFirst(expression);
        
        } else if (ctx.OR() != null) {
            
            Predicate right = this.predicates.removeFirst();
            Predicate left = this.predicates.removeFirst();
            
            Predicate expression = Predicates.or(left, right);
            this.predicates.addFirst(expression);
        }
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterInExpression(@NotNull InExpressionContext ctx) {
        
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
            
            this.predicates.addFirst(Predicates.notIn(fieldName, values));
            
        } else {
            
            this.predicates.addFirst(Predicates.in(fieldName, values));
        }
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterBetweenExpression(@NotNull BetweenExpressionContext ctx) {

        String fieldName = ctx.getChild(0).getText();
        
        boolean notBetween = (ctx.NOT() != null);
        
        if (notBetween) {
            
            String min = ctx.getChild(3).getText();
            String max = ctx.getChild(5).getText();
            
            this.predicates.addFirst(Predicates.notBetween(fieldName, min, max));
        
        } else {
            
            String min = ctx.getChild(2).getText();
            String max = ctx.getChild(4).getText();
            
            this.predicates.addFirst(Predicates.between(fieldName, min, max));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void enterSimpleExpression(@NotNull SimpleExpressionContext ctx) {

      String fieldName = ctx.ID().getText();
      Operator operator = Operator.fromSymbol(ctx.operator().getText());
      String value = ctx.value().getText();
    
      this.predicates.addFirst(Predicates.simplePredicate(fieldName, operator, value));
    }
}
