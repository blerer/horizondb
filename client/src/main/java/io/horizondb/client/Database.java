package io.horizondb.client;

import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.TimeSeriesId;
import io.horizondb.protocol.Msg;
import io.horizondb.protocol.OpCode;

/**
 * @author Benjamin
 *
 */
public class Database {

	private final ConnectionManager manager;
	
	/**
	 * The database definition.
	 */
	private final DatabaseDefinition definition;

	/**
	 * 
	 */
	Database(ConnectionManager manager, DatabaseDefinition definition) {

		this.manager = manager;
		this.definition = definition;
	}

	public TimeSeriesDefinition.Builder newTimeSeriesDefinitionBuilder(String seriesName) {
		
		return this.definition.newTimeSeriesDefinitionBuilder(seriesName);
	}
	
	/**
	 * Creates the specified time series.
	 * 
	 * @param timeSeriesDefinition the time series definition
	 * @return the time series corresponding to the specified definition.
	 */
	public TimeSeries createTimeSeries(TimeSeriesDefinition timeSeriesDefinition) {

		this.manager.send(Msg.newRequestMsg(OpCode.CREATE_TIMESERIES, timeSeriesDefinition));
		
		return new TimeSeries(this.manager, timeSeriesDefinition);
	}

	/**
	 * Returns the time series with the specified name.
	 * 
	 * @param seriesName the time series name
	 * @return the time series with the specified name.
	 */
    public TimeSeries getTimeSeries(String seriesName) {
    	
    	TimeSeriesId id = new TimeSeriesId(getName(), seriesName);
    	
    	Msg<TimeSeriesDefinition> response = this.manager.send(Msg.newRequestMsg(OpCode.GET_TIMESERIES, id));
	    
    	return new TimeSeries(this.manager, response.getPayload());
    }
	
	/**
	 * Returns the database name.
	 * @return the database name.
	 */
    public String getName() {
	    return this.definition.getName();
    }
}
