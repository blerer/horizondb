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
import io.horizondb.model.core.Field;

import java.util.TimeZone;

import com.google.common.collect.RangeSet;

/**
 * Expression that does nothing.
 * 
 * @author Benjamin
 *
 */
final class NoopExpression implements Expression {

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RangeSet<Field> getTimestampRanges(Field prototype, TimeZone timeZone) {
        
        return prototype.allValues();
    }
}
