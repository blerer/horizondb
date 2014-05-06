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
package io.horizondb.db.queries.expressions;

import java.util.List;

import io.horizondb.db.queries.Expression;

/**
 * Factory methods for Expressions.
 * 
 * @author Benjamin
 */
public final class Expressions {

    /**
     * Expression that does nothing.
     */
    private static final NoopExpression NOOP_EXPRESSION = new NoopExpression();
    
    /**
     * Creates simple a expression. 
     * 
     * @param fieldName the name of the field 
     * @param operator the operator 
     * @param value the value to which the value of the field must be compared
     * @return a simple a expression
     */
    public static Expression simpleExpression(String fieldName, Operator operator, String value) {
        return new SimpleExpression(fieldName, operator, value);
    }
    
    /**
     * Creates an EQUAL expression. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return an EQUAL expression
     */
    public static Expression eq(String fieldName, String value) {
        return simpleExpression(fieldName, Operator.EQ, value);
    }
    
    /**
     * Creates a NOT EQUAL expression. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a NOT EQUAL expression
     */
    public static Expression ne(String fieldName, String value) {
        return simpleExpression(fieldName, Operator.NE, value);
    }
    
    /**
     * Creates a GREATER THAN expression. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a GREATER THAN expression
     */
    public static Expression gt(String fieldName, String value) {
        return simpleExpression(fieldName, Operator.GT, value);
    }
    
    /**
     * Creates a GREATER OR EQUAL expression. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a GREATER OR EQUAL expression
     */
    public static Expression ge(String fieldName, String value) {
        return simpleExpression(fieldName, Operator.GE, value);
    }
    
    /**
     * Creates a LESS OR EQUAL expression. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a LESS OR EQUAL expression
     */
    public static Expression le(String fieldName, String value) {
        return simpleExpression(fieldName, Operator.LE, value);
    }
    
    /**
     * Creates a LESS THAN expression. 
     * 
     * @param fieldName the name of the field 
     * @param value the value to which the value of the field must be compared
     * @return a LESS THAN expression
     */
    public static Expression lt(String fieldName, String value) {
        return simpleExpression(fieldName, Operator.LT, value);
    }
    
    /**
     * Creates an AND expression. 
     * 
     * @param left the expression on the left hand side of the AND
     * @param right the expression on the right hand side of the AND
     * @return the AND expression
     */
    public static Expression and(Expression left, Expression right) {
        return new AndExpression(left, right);
    }
    
    /**
     * Creates an OR expression. 
     * 
     * @param left the expression on the left hand side of the OR
     * @param right the expression on the right hand side of the OR
     * @return the OR expression
     */
    public static Expression or(Expression left, Expression right) {
        return new OrExpression(left, right);
    }
    
    /**
     * Creates an IN expression. 
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @return the IN expression
     */
    public static Expression in(String fieldName, List<String> values) {
        return new InExpression(fieldName, values, false);
    }
    
    /**
     * Creates a NOT IN expression. 
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @return the NOT IN expression
     */
    public static Expression notIn(String fieldName, List<String> values) {
        return new InExpression(fieldName, values, true);
    }
    
    /**
     * Creates a BETWEEN expression. 
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a BETWEEN expression
     */
    public static Expression between(String fieldName, String min, String max) {
        return new BetweenExpression(fieldName, min, max, false);
    }
    
    /**
     * Creates a NOT BETWEEN expression. 
     * 
     * @param fieldName the name of the field
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a BETWEEN expression
     */
    public static Expression notBetween(String fieldName, String min, String max) {
        return new BetweenExpression(fieldName, min, max, true);
    }
    
    /**
     * Returns an expression that does nothing.
     * 
     * @return an expression that does nothing.
     */
    public static Expression noop() {
        return NOOP_EXPRESSION;
    }
    
    /**
     * Must not be instantiated.
     */
    private Expressions() {
        
    }
}
