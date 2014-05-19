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

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Query;
import io.horizondb.db.QueryContext;
import io.horizondb.db.databases.Database;
import io.horizondb.db.operations.ChunkedRecordStream;
import io.horizondb.db.series.TimeSeries;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;

import java.io.IOException;

/**
 * Query used to select a set of records from a specified time series. 
 * 
 * @author Benjamin
 *
 */
public final class SelectQuery implements Query {

    /**
     * The time series name.
     */
    private final String timeSeriesName;
    
    /**
     * The expression used to select the records returned by the query.
     */
    private final Expression expression;
        
    /**
     * Creates a <code>SelectQuery</code> used to return the records from the specified time series which match 
     * the specified criteria.
     * 
     * @param timeSeriesName the timeSeriesName of the database to use
     * @param expression the expression used to select the records
     */
    public SelectQuery(String timeSeriesName, Expression expression) {
        
        this.timeSeriesName = timeSeriesName;
        this.expression = expression;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object execute(QueryContext context) throws IOException, HorizonDBException {
        
        Database database = context.getDatabaseManager().getDatabase(context.getDatabaseName());

        TimeSeries series = database.getTimeSeries(this.timeSeriesName);
        
        RecordIterator iterator = series.read(this.expression);

        
        MsgHeader.newResponseHeader(context.getRequestHeader(), OpCode.DATA_CHUNK, 0, 0);
        return new ChunkedRecordStream(context.getRequestHeader(), iterator);
    }
    
    /**
     * Returns the name of the time series.    
     * 
     * @return the name of the time series
     */
    public String getTimeSeriesName() {
        return this.timeSeriesName;
    }
    
    /**
     * Returns the expression. 
     * 
     * @return the expression.
     */
    public Expression getExpression() {
        return this.expression;
    }
}
