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
import io.horizondb.model.Globals;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.core.fields.TimestampField;

import java.util.TimeZone;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.RangeSet;

/**
 * A simple expression used to compare a field to given value.
 * 
 * @author Benjamin
 */
final class SimpleExpression implements Expression {
    
    /**
     * The field name.
     */
    private final String fieldName;
    
    /**
     * The operator.
     */
    private final Operator operator;
    
    /**
     * The value to which the field value must be compared. 
     */
    private final String value;

    /**
     * Creates a new <code>SimpleExpression</code> instance for the specified field.
     * 
     * @param fieldName the field name
     * @param operator the operator
     * @param value the value to which the field value must be compared
     */
    public SimpleExpression(String fieldName, Operator operator, String value) {
        
        this.fieldName = fieldName;
        this.operator = operator;
        this.value = value;
    }

    /**
     * Returns the field name.
     * 
     * @return the field name.
     */
    public String getFieldName() {
        return this.fieldName;
    }

    /**
     * Returns the operator.
     * 
     * @return the operator.
     */
    public Operator getOperator() {
        return this.operator;
    }

    /**
     * Returns the value to which the field value must be compared.
     * @return the value to which the field value must be compared
     */
    public String getValue() {
        return this.value;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges(Field prototype, TimeZone timeZone) {
        
        if (!Globals.TIMESTAMP_COLUMN.equals(this.fieldName)) {
            return prototype.allValues();
        }
        
        TimestampField field = (TimestampField) prototype.newInstance();
        field.setValueFromString(timeZone, this.value);
        
        return this.operator.getRangeSet(ImmutableField.of(field));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        
        if (object == this) {
            return true;
        }
        if (!(object instanceof SimpleExpression)) {
            return false;
        }
        SimpleExpression rhs = (SimpleExpression) object;
        return new EqualsBuilder().append(this.fieldName, rhs.fieldName)
                                  .append(this.operator, rhs.operator)
                                  .append(this.value, rhs.value)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        
        return new HashCodeBuilder(-86018061, -1103579581).append(this.value)
                                                          .append(this.fieldName)
                                                          .append(this.operator)
                                                          .toHashCode();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new StringBuilder().append(this.fieldName)
                                  .append(' ')
                                  .append(this.operator)
                                  .append(' ')
                                  .append(this.value)
                                  .toString();
    }
}
