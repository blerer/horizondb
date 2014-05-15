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

import io.horizondb.db.series.Filter;
import io.horizondb.db.series.filters.Filters;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.TimeZone;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.collect.RangeSet;

/**
 * A simple expression used to compare a field to given value.
 * 
 * @author Benjamin
 */
final class SimpleExpression extends FieldExpression {
    
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
        
        super(fieldName);
        this.operator = operator;
        this.value = value;
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
        
        if (!isTimestamp()) {
            return prototype.allValues();
        }
        
        Field field = prototype.newInstance();
        field.setValueFromString(timeZone, this.value);
        
        return this.operator.getRangeSet(ImmutableField.of(field));
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Filter<Record> toFilter(TimeSeriesDefinition definition) {

        Field field = newField(definition, this.value);
        
        Filter<Field> fieldFilter = this.operator.getFilter(field, isTimestamp());
        
        return Filters.toRecordFilter(definition, getFieldName(), fieldFilter);
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
        return new EqualsBuilder().appendSuper(super.equals(object))
                                  .append(this.operator, rhs.operator)
                                  .append(this.value, rhs.value)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        
        return new HashCodeBuilder(-86018061, -1103579581).appendSuper(super.hashCode())
                .append(this.value)
                                                          .append(this.operator)
                                                          .toHashCode();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new StringBuilder().append(getFieldName())
                                  .append(' ')
                                  .append(this.operator)
                                  .append(' ')
                                  .append(this.value)
                                  .toString();
    }
}
