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

import static java.lang.String.format;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.parser.BadHqlGrammarException;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.core.predicates.Operator;
import io.horizondb.model.core.predicates.Predicates;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Factory methods for <code>PredicateBuilder</code>.
  */
public final class PredicateBuilders {

    /**
     * Builder for NOOP predicates.
     */
    private static final PredicateBuilder NOOP_BUILDER = new PredicateBuilder() {

        @Override
        public Predicate build(TimeSeriesDefinition definition) {
            return Predicates.noop();
        }
    };
    
    /**
     * Creates a builder for a NOOP predicate.
     *
     * @return a builder for an NOOP predicate
     */
    public static PredicateBuilder noop() {
        return NOOP_BUILDER;
    }
    
    /**
     * Creates a builder for an AND predicate.
     *
     * @param left the builder for the left predicate
     * @param right the builder for the right predicate
     * @return a builder for an AND predicate
     */
    public static PredicateBuilder and(final PredicateBuilder left, 
                                       final PredicateBuilder right) {
        
        return new PredicateBuilder() {

            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                return Predicates.and(left.build(definition), right.build(definition));
            }
        };
    }
    
    /**
     * Creates a builder for an OR predicate.
     *
     * @param left the builder for the left predicate
     * @param right the builder for the right predicate
     * @return a builder for an OR predicate
     */
    public static PredicateBuilder or(final PredicateBuilder left, 
                                      final PredicateBuilder right) {
        
        return new PredicateBuilder() {

            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                return Predicates.or(left.build(definition), right.build(definition));
            }
        };
    }

    /**
     * Creates a builder for an IN predicate. 
     * 
     * @param fieldName the name of the field 
     * @param values the values
     * @return a builder for a IN predicate
     */
    public static PredicateBuilder in(final String fieldName, 
                                      final List<String> values) {
        return new PredicateBuilder() {
            
            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                
                return Predicates.in(fieldName, getFields(definition, fieldName, values));
            }
        };
    }

    /**
     * Creates a builder for a NOT IN predicate. 
     * 
     * @param fieldName the name of the field 
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a builder for a NOT IN predicate
     */
    public static PredicateBuilder notIn(final String fieldName, 
                                         final List<String> values) {
        return new PredicateBuilder() {
            
            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                
                return Predicates.notIn(fieldName, getFields(definition, fieldName, values));
            }
        };
    }
    
    /**
     * Creates a builder for a BETWEEN predicate. 
     * 
     * @param fieldName the name of the field 
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a builder for a BETWEEN predicate
     */
    public static PredicateBuilder between(final String fieldName, 
                                           final String min,
                                           final String max) {
        return new PredicateBuilder() {
            
            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                
                return Predicates.between(fieldName, 
                                          getField(definition, fieldName, min),
                                          getField(definition, fieldName, max));
            }
        };
    }

    /**
     * Creates a builder for a NOT BETWEEN predicate. 
     * 
     * @param fieldName the name of the field 
     * @param min the minimum value of the closed range
     * @param max the maximum value of the closed range
     * @return a builder for a NOT BETWEEN predicate
     */
    public static PredicateBuilder notBetween(final String fieldName, 
                                              final String min,
                                              final String max) {
        return new PredicateBuilder() {
            
            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                
                return Predicates.notBetween(fieldName, 
                                             getField(definition, fieldName, min),
                                             getField(definition, fieldName, max));
            }
        };
    }
    
    /**
     * Creates a builder for a simple a predicate. 
     * 
     * @param fieldName the name of the field 
     * @param operator the operator 
     * @param value the value to which the value of the field must be compared
     * @return a simple a predicate builder
     */
    public static PredicateBuilder simplePredicate(final String fieldName, 
                                                   final Operator operator,
                                                   final String value) {
        return new PredicateBuilder() {
            
            @Override
            public Predicate build(TimeSeriesDefinition definition) throws HorizonDBException {
                
                return Predicates.simplePredicate(fieldName,
                                                  operator,
                                                  getField(definition, fieldName, value));
            }
        };
    }

    /**
     * Returns the field with the specified name and the specified value. 
     *
     * @param definition the time series definition
     * @param fieldName the field name
     * @param value the field value
     * @return the field with the specified name and the specified value
     * @throws HorizonDBException if the field cannot be created
     */
    private static Field getField(TimeSeriesDefinition definition, 
                                  String fieldName,
                                  String value) throws HorizonDBException {
        
        return setFieldValue(definition, newField(definition, fieldName), value);
    }

    /**
     * Returns the fields with specified values. 
     *
     * @param definition the time series definition
     * @param fieldName the field name
     * @param values the field values
     * @return the fields with specified values
     * @throws HorizonDBException if the fields cannot be created
     */
    private static SortedSet<Field> getFields(TimeSeriesDefinition definition,
                                              String fieldName,
                                              List<String> values) throws HorizonDBException {
        
        Field prototype = newField(definition, fieldName);
        SortedSet<Field> fields = new TreeSet<>();
        for (int i = 0, m = values.size(); i < m; i++) {
            fields.add(setFieldValue(definition, prototype.newInstance(), values.get(i)));
        }
        return fields;
    }
    
    /**
     * Sets the value of the specified field.
     *
     * @param definition the time series definition
     * @param field the field
     * @param value the value as <code>String</code>
     * @return the field with the value set
     * @throws BadHqlGrammarException if the value is invalid for the specified field
     */
    private static Field setFieldValue(TimeSeriesDefinition definition, Field field, String value) throws BadHqlGrammarException {
        
        try {
            
            return field.setValueFromString(definition.getTimeZone(), value);

        } catch (NumberFormatException e) {
            throw new BadHqlGrammarException(format("The value %s cannot be converted into a number", value));
        } catch (IllegalArgumentException e) {
            throw new BadHqlGrammarException(e.getMessage());
        }
    }

    /**
     * Creates a new instance of the field with the specified name. 
     *
     * @param definition the time series definition
     * @param fieldName the field name
     * @return a new instance of the field with the specified name. 
     * @throws HorizonDBException if no field with the specified name exists
     */
    private static Field newField(TimeSeriesDefinition definition, String fieldName) throws HorizonDBException {
        
        Field field = definition.newField(fieldName);

        if (field == null)
        {
            throw new HorizonDBException(ErrorCodes.INVALID_QUERY, format("Unknown field: %s", fieldName));
        }
        return field;
    }
    
    /**
     * The class must not be instantiated.
     */
    private PredicateBuilders() {
    }
}
