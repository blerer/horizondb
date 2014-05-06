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
import io.horizondb.db.parser.HqlParser.ExpressionContext;
import io.horizondb.db.parser.HqlParser.InExpressionContext;
import io.horizondb.db.parser.HqlParser.SelectContext;
import io.horizondb.db.parser.HqlParser.SimpleExpressionContext;
import io.horizondb.db.parser.QueryBuilder;
import io.horizondb.db.queries.Expression;
import io.horizondb.db.queries.SelectQuery;
import io.horizondb.db.queries.expressions.Expressions;
import io.horizondb.db.queries.expressions.Operator;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

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
     * The expressions.
     */
    private Deque<Expression> expressions = new LinkedList<>();
        
    /**
     * 
     */
    public SelectQueryBuilder() {
        this.expressions.addFirst(Expressions.noop());
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
    public Query build() {

        return new SelectQuery(this.timeSeriesName, this.expressions.poll());
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
            
            Expression right = this.expressions.removeFirst();
            Expression left = this.expressions.removeFirst();
            
            Expression expression = Expressions.and(left, right);
            
            this.expressions.addFirst(expression);
        
        } else if (ctx.OR() != null) {
            
            Expression right = this.expressions.removeFirst();
            Expression left = this.expressions.removeFirst();
            
            Expression expression = Expressions.or(left, right);
            this.expressions.addFirst(expression);
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
            
            this.expressions.addFirst(Expressions.notIn(fieldName, values));
            
        } else {
            
            this.expressions.addFirst(Expressions.in(fieldName, values));
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
            
            this.expressions.addFirst(Expressions.notBetween(fieldName, min, max));
        
        } else {
            
            String min = ctx.getChild(2).getText();
            String max = ctx.getChild(4).getText();
            
            this.expressions.addFirst(Expressions.between(fieldName, min, max));
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
    
      this.expressions.addFirst(Expressions.simpleExpression(fieldName, operator, value));
    }
}
