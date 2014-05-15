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

import java.util.TimeZone;

import io.horizondb.db.queries.Expression;
import io.horizondb.model.Globals;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.schema.TimeSeriesDefinition;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Base class for expression applying to one field.
 * 
 * @author Benjamin
 *
 */
public abstract class FieldExpression implements Expression {
    
    /**
     * The field name.
     */
    private final String fieldName;

    /**
     * Creates a new <code>FieldExpression</code> that apply to the specified field.
     * 
     * @param fieldName the field name
     */
    public FieldExpression(String fieldName) {
        this.fieldName = fieldName;
    }
    
    /**
     * Returns the field name.
     * 
     * @return the field name.
     */
    public final String getFieldName() {
        return this.fieldName;
    }

    /**
     * Returns <code>true</code> if the field is the timestamp field, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if the field is the timestamp field, <code>false</code> otherwise.
     */
    protected final boolean isTimestamp() {
        
        return Globals.TIMESTAMP_FIELD.equals(this.fieldName);
    }
    
    /**
     * Returns a new field instance. 
     * 
     * @param definition the definition of the time series to which belong the field 
     * @return a new field instance with the specified value. 
     */
    protected final Field newField(TimeSeriesDefinition definition) {

        return definition.newField(this.fieldName);
    }
    
    /**
     * Returns a new field instance with the specified value. 
     * 
     * @param definition the definition of the time series to which belong the field 
     * @param value the field value
     * @return a new field instance with the specified value. 
     */
    protected final Field newField(TimeSeriesDefinition definition, String value) {
        
        Field field = newField(definition);
        field.setValueFromString(definition.getTimeZone(), value);
        
        return ImmutableField.of(field);
    }
    
    
    /**
     * Returns a new field instance with the specified value. 
     * 
     * @param prototype the prototype used to create the new field.
     * @param timeZone the time series time zone
     * @param value the field value
     * @return a new field instance with the specified value. 
     */
    protected static final Field newField(Field prototype, TimeZone timeZone, String value) {
        
        Field field = prototype.newInstance(timeZone, value);        
        return ImmutableField.of(field);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof FieldExpression)) {
            return false;
        }
        FieldExpression rhs = (FieldExpression) object;
        return new EqualsBuilder().append(this.fieldName, rhs.fieldName).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-19009959, 916673691).append(this.fieldName)
                                                        .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("fieldName", this.fieldName)
                                                                          .toString();
    }
}
