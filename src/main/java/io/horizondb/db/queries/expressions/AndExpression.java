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

import io.horizondb.db.queries.Expression;

/**
 * An AND expression
 * 
 * @author Benjamin
 */
final class AndExpression extends LogicalExpression {

    /**
     * Creates an <code>AndExpression</code> 
     * 
     * @param left the expression on the left hand side of the AND.
     * @param right the expression on the right hand side of the AND.
     */
    public AndExpression(Expression left, Expression right) {
        
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getOperatorAsString() {
        return "AND";
    }
}
