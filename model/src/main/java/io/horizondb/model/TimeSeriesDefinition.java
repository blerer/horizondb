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
package io.horizondb.model;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.io.serialization.Parser;
import io.horizondb.io.serialization.Serializable;
import io.horizondb.io.serialization.Serializables;
import io.horizondb.model.records.BinaryTimeSeriesRecord;
import io.horizondb.model.records.TimeSeriesRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import static org.apache.commons.lang.Validate.notNull;

/**
 * The definition of a time series stored in the databaseName.
 * 
 * @author Benjamin
 */
@Immutable
public final class TimeSeriesDefinition implements Serializable {

	/**
	 * The parser instance.
	 */
	private static final Parser<TimeSeriesDefinition> PARSER = new Parser<TimeSeriesDefinition>() {
	
		/**
		 * {@inheritDoc}
		 */
		@Override
		public TimeSeriesDefinition parseFrom(ByteReader reader) throws IOException {
        	
			String databaseName = VarInts.readString(reader);
			String name = VarInts.readString(reader);
			TimeUnit timestampUnit = TimeUnit.values()[reader.readByte()];
			TimeZone timeZone = TimeZone.getTimeZone(VarInts.readString(reader));
			PartitionType partitionType = PartitionType.parseFrom(reader);
			Serializables<RecordTypeDefinition> recordTypes = Serializables.parseFrom(RecordTypeDefinition.getParser(), reader);
        	
        	return new TimeSeriesDefinition(databaseName, name, timestampUnit, timeZone, partitionType, recordTypes);
		}
	};	
	
	/**
	 * The seriesName of the time series'databaseName.
	 */
	private final String databaseName;
	
	/**
	 * The seriesName of the time series.
	 */
	private final String seriesName;
	
	/**
	 * The unit of time of the series. 
	 */
	private final TimeUnit timeUnit;
	
	/**
	 * The time series timeZone.
	 */
	private final TimeZone timeZone;
	
	/**
	 * The partitionType type.
	 */
	private final PartitionType partitionType;
	
	/**
	 * The type of records composing this time series.
	 */
	private final Serializables<RecordTypeDefinition> recordTypes;
			
	/**
	 * {@inheritDoc}
	 */
	@Override
    public int computeSerializedSize() {

	    return VarInts.computeStringSize(this.databaseName) 
	    		+ VarInts.computeStringSize(this.seriesName) 
	    		+ 1 // TimeUnit
	    		+ VarInts.computeStringSize(this.timeZone.getID())
	    		+ this.partitionType.computeSerializedSize()
	    		+ this.recordTypes.computeSerializedSize();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void writeTo(ByteWriter writer) throws IOException {

		VarInts.writeString(writer, this.databaseName);
		VarInts.writeString(writer, this.seriesName);
		VarInts.writeByte(writer, this.timeUnit.ordinal());
		VarInts.writeString(writer, this.timeZone.getID());
		this.partitionType.writeTo(writer);
		this.recordTypes.writeTo(writer);
    }
	
	/**
	 * Returns binary records instances corresponding to this time series records. 
	 * @return binary records instances corresponding to this time series records. 
	 */
	public BinaryTimeSeriesRecord[] newBinaryRecords() {
		
		int numberOfTypes = this.recordTypes.size();
		
		BinaryTimeSeriesRecord[] records = new BinaryTimeSeriesRecord[numberOfTypes];
		
		for (int i = 0; i < numberOfTypes; i++) {
	        
			records[i] = this.recordTypes.get(i).newBinaryRecord(i, this.timeUnit);
        }
		
		return records;
	}
	
	/**
	 * Returns records instances corresponding to this time series records. 
	 * @return records instances corresponding to this time series records. 
	 */
	public TimeSeriesRecord[] newRecords() {
		
		int numberOfTypes = this.recordTypes.size();
		
		TimeSeriesRecord[] records = new TimeSeriesRecord[numberOfTypes];
		
		for (int i = 0; i < numberOfTypes; i++) {
	        
			records[i] = this.recordTypes.get(i).newRecord(i, this.timeUnit);
        }
		
		return records;
	}
	
	/**
	 * Returns the time range of the partition to which belongs the specified time.
	 * 
	 * @param timestampInMillis the timestamp in millisecond
	 * @return the time range of the partition to which belongs the specified time.
	 */
	public TimeRange getPartitionTimeRange(long timestampInMillis) {
		
		Calendar calendar = Calendar.getInstance(this.timeZone);
		calendar.setTimeInMillis(timestampInMillis);

		return this.partitionType.getPartitionTimeRange(calendar);
	}
	
	/**
	 * Returns a new record instances of the specified type. 
	 * 
	 * @param type the name of the record type. 
	 * @return a new record instances of the specified type. 
	 */
	TimeSeriesRecord newRecord(int index) {

		Validate.isTrue(index >= 0 && index <= this.recordTypes.size(), "No record has been defined for the index: "
		        + index);

		return this.recordTypes.get(index).newRecord(index, this.timeUnit);
	}
	
	/**
	 * Returns the index of the specified record type.
	 * 
	 * @param type the record type.
	 * @return the index of the specified record type.
	 */
	int getRecordTypeIndex(String type) {
		
		for (int i = 0, m = this.recordTypes.size(); i < m; i++) {
			
			RecordTypeDefinition recordType = this.recordTypes.get(i);
			
			if (recordType.getName() == type) {
				
				return i;
			}
		}
		
		throw new IllegalArgumentException("No " + type + " records have not been defined within the " 
				+ this.seriesName + " time series.");
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object object) {
	    if (object == this) {
		    return true;
	    }
	    if (!(object instanceof TimeSeriesDefinition)) {
		    return false;
	    }
	    TimeSeriesDefinition rhs = (TimeSeriesDefinition) object;
	    return new EqualsBuilder().append(this.recordTypes, rhs.recordTypes)
	                              .append(this.timeUnit, rhs.timeUnit)
	                              .append(this.databaseName, rhs.databaseName)
	                              .append(this.seriesName, rhs.seriesName)
	                              .append(this.timeZone, rhs.timeZone)
	                              .append(this.partitionType, rhs.partitionType)
	                              .isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
	    return new HashCodeBuilder(510479313, 1641137635).append(this.recordTypes)
	                                                     .append(this.timeUnit)
	                                                     .append(this.seriesName)
	                                                     .append(this.databaseName)
	                                                     .append(this.timeZone)
	                                                     .append(this.partitionType)
	                                                     .toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("databaseName", this.databaseName)
																		  .append("seriesName", this.seriesName)
		                                                                  .append("timestampUnit", this.timeUnit)
		                                                                  .append("timeZone", this.timeZone)
		                                                                  .append("partitionType", this.partitionType)
		                                                                  .append("recordTypes", this.recordTypes)
		                                                                  .toString();
	}
	
	/**
	 * Creates a new <code>TimeSeriesMetaData</code> by reading the data from the specified reader.
	 * 
	 * @param reader the reader to read from.
	 * @throws IOException if an I/O problem occurs
	 */
	public static TimeSeriesDefinition parseFrom(ByteReader reader) throws IOException {
		
		return getParser().parseFrom(reader);
	}
	
	/**
	 * Returns the parser that can be used to deserialize <code>TimeSeriesMetaData</code> instances.
	 * @return the parser that can be used to deserialize <code>TimeSeriesMetaData</code> instances.
	 */
    public static Parser<TimeSeriesDefinition> getParser() {

	    return PARSER;
    }
	   
    /**
     * Returns the database seriesName.
     * 
     * @return the database seriesName.
     */
    public String getDatabaseName() {
		return this.databaseName;
	}
    
    /**
     * Returns the time series seriesName.
     * 
     * @return the time series seriesName.
     */
    public String getSeriesName() {
		return this.seriesName;
	}

    /**
     * Returns the time unit of the time series timestamps.
     * 
     * @return the time unit of the time series timestamps.
     */
    public TimeUnit getTimeUnit() {
		return this.timeUnit;
	}

    /**
     * Returns the timezone of the time series.
     * 
     * @return the timezone of the time series.
     */
    public TimeZone getTimeZone() {
		return this.timeZone;
	}

    /**
     * Returns the partitionType type of the time series.
     * 
     * @return the partitionType type of the time series.
     */
    public PartitionType getPartitionType() {
		return this.partitionType;
	}
	
	/**
	 * Returns the number of record types.
	 * @return the number of record types.
	 */
	public int getNumberOfRecordTypes() {
		
		return this.recordTypes.size();
	}
		
	/**
	 * Creates a new <code>Builder</code> instance.
	 * 
	 * @param seriesName the time series seriesName
	 * @return a new <code>Builder</code> instance.
	 */
	static Builder newBuilder(String databaseName, String name) {
		
		return new Builder(databaseName, name);
	}
    
	/**
	 * Creates a new <code>TimeSeriesMetaData</code> using the specified builder.
	 * 
	 * @param builder the builder.
	 */
	private TimeSeriesDefinition(Builder builder) {
		
		this(builder.databaseName, builder.name, builder.timeUnit, builder.timeZone, builder.partitionType, new Serializables<>(builder.recordTypes));
	}
	
	private TimeSeriesDefinition(String database,
						         String name,
						         TimeUnit timeUnit,
						         TimeZone timeZone,
						         PartitionType partitionType,
						         Serializables<RecordTypeDefinition> recordTypes) {

		this.databaseName = database;
		this.seriesName = name;
		this.timeUnit = timeUnit;
		this.timeZone = timeZone;
		this.partitionType = partitionType;
		this.recordTypes = recordTypes;
	}
	
	/**
	 * Builds instance of <code>TimeSerieDefinition</code>.
	 */
	public static class Builder {
				
		/**
		 * The seriesName of the timeseries'databaseName.
		 */
		private final String databaseName;
		
		/**
		 * The time series seriesName.
		 */
		private final String name;
		
		/**
		 * The unit of time of the series. 
		 */
		private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
		
		/**
		 * The time series timeZone.
		 */
		private TimeZone timeZone = TimeZone.getDefault();
		
		/**
		 * The partitionType type.
		 */
		private PartitionType partitionType = PartitionType.BY_DAY;
		
		
		/**
		 * The type of records that will be composing the time series.
		 */
		private final List<RecordTypeDefinition> recordTypes = new ArrayList<>();
		
		/**
		 * Must not be called from outside the enclosing class.
		 */
		private Builder(String database, String name) {
			
			Validate.notEmpty(database, "the databaseName seriesName must not be empty.");
			Validate.notEmpty(name, "the time series seriesName must not be empty.");

			this.databaseName = database;
			this.name = name;
		}
		
		/**
		 * Sets the time unit of the time series.
		 * 
		 * @param timeUnit the time unit of the time series.
		 * @return this <code>Builder</code>.
		 */
		public Builder timeUnit(TimeUnit timeUnit) {
			
			notNull(timeUnit, "the timeUnit parameter must not be null.");
			
			this.timeUnit = timeUnit;
			return this;
		}
		
		/**
		 * Sets the time zone of the time series.
		 * 
		 * @param timeZone the time zone of the time series.
		 * @return this <code>Builder</code>.
		 */
		public Builder timeZone(TimeZone timeZone) {
			
			notNull(timeZone, "the timeZone parameter must not be null.");
			
			this.timeZone = timeZone;
			return this;
		}
		
		/**
		 * Sets the way the time series must be partitioned.
		 * 
		 * @param partitionType the way the time series must be partitioned.
		 * @return this <code>Builder</code>.
		 */
		public Builder partitionType(PartitionType partitionType) {
			
			notNull(partitionType, "the partitionType parameter must not be null.");
			
			this.partitionType = partitionType;
			return this;
		}
		
		/**
		 * Adds the specified record type to the type of records that will be composing the time series.
		 * 
		 * @param builder the builder of the record type to add.
		 * @return this <code>Builder</code>.
		 */
		public Builder addRecordType(RecordTypeDefinition.Builder builder) {
			
			return addRecordType(builder.build());
		}
		
		/**
		 * Adds the specified record type to the type of records that will be composing the time series.
		 * 
		 * @param recordType the record type to add.
		 * @return this <code>Builder</code>.
		 */
		public Builder addRecordType(RecordTypeDefinition recordType) {
			
			this.recordTypes.add(recordType);
			return this;
		}
		
		/**
		 * Creates a new <code>TimeSeriesMetaData</code> instance.
		 * 
		 * @return a new <code>TimeSeriesMetaData</code> instance.
		 */
		public TimeSeriesDefinition build() {
			
			Validate.notEmpty(this.recordTypes, "no record type has been specified");
			
			return new TimeSeriesDefinition(this);
		}
	}
}
