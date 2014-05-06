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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.text.StrBuilder;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableRangeSet.Builder;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * An IN expression.
 * 
 * @author Benjamin
 *
 */
final class InExpression implements Expression {

    /**
     * The name of the field that should be compared to the values.
     */
    private final String fieldName;
    
    /**
     * The values to which must be field must be compared.
     */
    private final List<String> values;
    
    /**
     * <code>true</code> if the operator is a NOT IN operator.
     */
    private final boolean notIn;
    
    /**
     * Creates an IN expression.
     * 
     * @param fieldName the name of the field
     * @param values the values
     */
    public InExpression(String fieldName, List<String> values) {
        
        this(fieldName, values, false);
    }

    /**
     * Creates an IN expression.
     * 
     * @param fieldName the name of the field
     * @param values the values
     * @param notIn <code>true</code> if the field value must not be within the specified values
     */
    public InExpression(String fieldName, List<String> values, boolean notIn) {
        
        this.fieldName = fieldName;
        this.values = values;
        this.notIn = notIn;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges(Field prototype, TimeZone timeZone) {
        
        if (!Globals.TIMESTAMP_COLUMN.equals(this.fieldName)) {
            return prototype.allValues();
        }
        
        List<Field> fields = new ArrayList<>();
        
        for (int i = 0, m = this.values.size(); i < m; i++) {
            
            String value = this.values.get(i);
            
            TimestampField field = (TimestampField) prototype.newInstance();
            field.setValueFromString(timeZone, value);
            
            fields.add(ImmutableField.of(field));
        }
        
        Collections.sort(fields);
        
        Builder<Field> builder = ImmutableRangeSet.builder();
 
        for (int i = 0, m = fields.size(); i < m; i++) {

            Field field = fields.get(i);
            builder.add(Range.closed(field, field));
        }
        
        RangeSet<Field> rangeSet = builder.build();
        
        if (this.notIn) {
            return rangeSet.complement();
        }
        
        return rangeSet;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        StrBuilder builder = new StrBuilder().append(this.fieldName)
                                             .append(' ');
        
        if (this.notIn) {
            
            builder.append("NOT ");
        }
        
        return builder.append("IN (")
                      .appendWithSeparators(this.values, ", ")
                      .append(")")
                      .toString();
    }
}
