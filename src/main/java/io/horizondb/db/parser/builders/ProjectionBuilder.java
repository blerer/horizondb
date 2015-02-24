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

import io.horizondb.model.core.Projection;
import io.horizondb.model.core.projections.DefaultProjection;
import io.horizondb.model.core.projections.DefaultRecordTypeProjection;
import io.horizondb.model.core.projections.NoopProjection;
import io.horizondb.model.core.projections.NoopRecordTypeProjection;
import io.horizondb.model.core.projections.RecordTypeProjection;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Builder for Projections.
 *
 */
final class ProjectionBuilder {

    private final List<String> selection;
    
    /**
     * Creates a new <code>ProjectionBuilder</code> for the specified selection
     * @param selection the selection
     */
    public ProjectionBuilder(List<String> selection) {
        this.selection = selection;
    }

    public Projection build(TimeSeriesDefinition definition) {
        
        if (isSelectAll(this.selection)) {
            return new NoopProjection();
        }
        
        ListMultimap<String, String> multimap = convertToMultiMap(this.selection);
        
        List<RecordTypeProjection> projections = new ArrayList<>(multimap.size());
        
        for (String recordType: multimap.keySet()) {
            
            int index = definition.getRecordTypeIndex(recordType);
            List<String> fields = multimap.get(recordType);
            projections.add(toRecordTypeProjection(index, fields));
        }
        
        return new DefaultProjection(projections);
    }

    /**
     * Converts the specified selection into a <code>MultiMap</code>
     * @return a MultiMap containing field names per record type name 
     */
    private static ListMultimap<String, String> convertToMultiMap(List<String> selection) {
        
        ListMultimap<String, String> multimap = ArrayListMultimap.create();
        
        for (String expression : selection) {
            String[] elements = expression.split("\\.");
            String record = elements[0];
            String field = elements[1]; 
            multimap.put(record, field);
        }
        
        return multimap;
    }

    /**
     * @param index
     * @param fields
     * @return
     */
    private static RecordTypeProjection toRecordTypeProjection(int index, List<String> fields) {

        if (isSelectAll(fields)) {
            return new NoopRecordTypeProjection(index);
        }
        return new DefaultRecordTypeProjection(index, fields);
    }

    /**
     * Checks if the specified selection select everything.
     * @return <code>true</code> if the specified selection select everything, <code>false</code>
     * otherwise.
     */
    private static boolean isSelectAll(List<String> selection) {
        return selection.size() == 1 && selection.contains("*");
    }
}
