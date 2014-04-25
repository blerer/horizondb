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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Range;

/**
 * Sets of criterion.
 * 
 * @author Benjamin
 *
 */
public final class Criteria {

    /**
     * The criterion list.
     */
    private List<Criterion> criterionList = new ArrayList<>();
    
    /**
     * @return
     */
    public Range<Long> getTimestampRange() {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Returns <code>true</code> if this <code>Criteria</code> is empty, <code>false</code>
     * otherwise.
     * 
     * @return <code>true</code> if this <code>Criteria</code> is empty, <code>false</code>
     * otherwise.
     */
    public boolean isEmpty() {
        return this.criterionList.isEmpty();
    }
    
    /**
     * Creates a new <code>Builder</code> instance.
     * 
     * @return a new <code>Builder</code> instance.
     */
    public static Builder newBuilder() {

        return new Builder();
    }

    /**
     * Creates a new <code>Criteria</code> instance using the specified <code>Builder</code>.
     * 
     * @param builder the builder used to create this <code>Criteria</code>.
     */
    private Criteria(Builder builder) {
        this.criterionList = builder.criteria;
    }

    /**
     * Builds instance of <code>Criteria</code>.
     *
     */
    public static class Builder {

        /**
         * The criterionList.
         */
        private List<Criterion> criteria = new ArrayList<>();
        
        /**
         * Adds the specified criterion to the criterionList.
         * 
         * @param criterion the criterion to add.
         */
        public void add(Criterion criterion) {
            
            this.criteria.add(criterion);
        }
        
        /**
         * Creates a new <code>Criteria</code> instance.
         * 
         * @return a new <code>Criteria</code> instance.
         */
        public Criteria build() {

            return new Criteria(this);
        }

        /**
         * Must not be called from outside the enclosing class.
         */
        private Builder() {

        }
    }
}
