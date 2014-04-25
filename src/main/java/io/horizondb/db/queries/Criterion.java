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
package io.horizondb.db.queries;

/**
 * A criterion used to specify a set of records.
 * 
 * @author Benjamin
 *
 */
public final class Criterion {
    
    /**
     * The name of the record field to which this criterion apply.
     */
    private final String fieldName;
    
    /**
     * The operator.
     */
    private final Operator operator;
    
    /**
     * The value to which the field must be compared. 
     */
    private final String value;

    /**
     * Creates a new <code>Criterion</code> instance for the specified field.
     * 
     * @param fieldName the name of the record field to which this criterion apply
     * @param operator the operator
     * @param value the value to which the field must be compared
     */
    public Criterion(String fieldName, Operator operator, String value) {
        
        this.fieldName = fieldName;
        this.operator = operator;
        this.value = value;
    }

    /**
     * Returns the name of the record field to which this criterion apply.
     * 
     * @return he name of the record field to which this criterion apply.
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
     * Returns the value to which the field must be compared.
     * @return the value to which the field must be compared
     */
    public String getValue() {
        return this.value;
    }
}
