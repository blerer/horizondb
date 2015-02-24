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

import io.horizondb.db.HorizonDBException;
import io.horizondb.model.core.Predicate;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * A <code>Builder</code> for predicates.
 */
public interface PredicateBuilder {

    /**
     * Builds a new <code>Predicate</code> instance.
     * 
     * @param definition the definition of the time series on which this predicate must be applied
     * @return a new <code>Predicate</code> instance
     * @throws HorizonDBException if the predicate cannot be build due to some invalid values.
     */
    Predicate build(TimeSeriesDefinition definition) throws HorizonDBException;
}
