/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.db.operations;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Operation;
import io.horizondb.db.OperationContext;
import io.horizondb.db.databases.Database;
import io.horizondb.db.series.TimeSeries;
import io.horizondb.db.series.filters.Filters;
import io.horizondb.model.Globals;
import io.horizondb.model.core.Field;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.Msgs;
import io.horizondb.model.protocol.QueryPayload;

import java.io.IOException;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * @author Benjamin
 * 
 */
final class QueryOperation implements Operation {

    /**
     * {@inheritDoc}
     */
    @Override
    public Object perform(OperationContext context, Msg<?> request) throws IOException, HorizonDBException {

        QueryPayload queryPayload = Msgs.getPayload(request);

        Database database = context.getDatabaseManager().getDatabase(queryPayload.getDatabaseName());

        TimeSeries series = database.getTimeSeries(queryPayload.getSeriesName());

        Field prototype = series.getDefinition().newField(Globals.TIMESTAMP_FIELD);
        
        Range<Long> range = queryPayload.getTimeRange();
        
        Field lower = prototype.newInstance();
        lower.setTimestampInMillis(range.lowerEndpoint().longValue());
        
        Field upper = prototype.newInstance();
        upper.setTimestampInMillis(range.upperEndpoint().longValue());
        
        RangeSet<Field> timeRanges = ImmutableRangeSet.of(Range.closedOpen(lower, upper));
        
        RecordIterator iterator = series.read(timeRanges, Filters.<Record>noop());

        return new ChunkedRecordStream(request.getHeader(), iterator);
    }
}
