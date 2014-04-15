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

import io.horizondb.db.Query;
import io.horizondb.db.parser.HqlBaseListener;
import io.horizondb.db.parser.HqlParser.CreateTimeseriesContext;
import io.horizondb.db.parser.HqlParser.FieldDefinitionContext;
import io.horizondb.db.parser.HqlParser.RecordDefinitionContext;
import io.horizondb.db.parser.QueryBuilder;
import io.horizondb.db.queries.CreateTimeSeriesQuery;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import org.antlr.v4.runtime.misc.NotNull;

/**
 * <code>Builder</code> for <code>CreateTimeseriesQuery</code> instances.
 * 
 * @author Benjamin
 *
 */
final class CreateTimeseriesQueryBuilder extends HqlBaseListener implements QueryBuilder {

    /**
     * The time series definition builder.
     */
    private TimeSeriesDefinition.Builder timeSeriesDefBuilder;
    
    /**
     * The record type builder.
     */
    private RecordTypeDefinition.Builder recordTypeDefBuilder;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void enterRecordDefinition(@NotNull RecordDefinitionContext ctx) {
       
        String recordTypeName = ctx.ID().getText();
        this.recordTypeDefBuilder = RecordTypeDefinition.newBuilder(recordTypeName); 
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void exitRecordDefinition(@NotNull RecordDefinitionContext ctx) {
        
        this.timeSeriesDefBuilder.addRecordType(this.recordTypeDefBuilder);
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterFieldDefinition(@NotNull FieldDefinitionContext ctx) {
        
        String fieldName = ctx.ID().getText();
        String fieldType = ctx.type().getText();
                        
        this.recordTypeDefBuilder.addField(fieldName, FieldType.valueOf(fieldType));
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public void enterCreateTimeseries(@NotNull CreateTimeseriesContext ctx) {
        
        String timeSeriesName = ctx.ID().getText();
        this.timeSeriesDefBuilder = TimeSeriesDefinition.newBuilder(timeSeriesName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Query build() {

        return new CreateTimeSeriesQuery(this.timeSeriesDefBuilder.build());
    }
}
