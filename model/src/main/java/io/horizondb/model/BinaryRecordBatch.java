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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.serialization.Parser;

import java.io.IOException;

/**
 * @author Benjamin
 *
 */
public class BinaryRecordBatch extends AbstractRecordBatch {

	private static final Parser<BinaryRecordBatch> PARSER = 
			new AbstractRecordBatch.AbstractParser<BinaryRecordBatch, ReadableBuffer>() {

		@Override
		protected ReadableBuffer parseRecordSetFrom(ByteReader reader) throws IOException {

			ReadableBuffer readableBuffer = (ReadableBuffer) reader;

			return readableBuffer.slice(readableBuffer.readableBytes());
		}

		@Override
		protected BinaryRecordBatch newRecordBatch(String databaseName,
		                                           String seriesName,
		                                           long partition,
		                                           ReadableBuffer buffer) {

			return new BinaryRecordBatch(databaseName, seriesName, partition, buffer);
		}
	};
	
	private final ReadableBuffer buffer;
	
	/**
	 * @param databaseName
	 * @param seriesName
	 * @param partition
	 * @param buffer
	 */
    public BinaryRecordBatch(String databaseName, String seriesName, long partition, ReadableBuffer buffer) {
	    super(databaseName, seriesName, partition);
	    this.buffer = buffer;
    }

    public static Parser<BinaryRecordBatch> getParser() {
    	
    	return PARSER;
    }
    
    public static BinaryRecordBatch parseFrom(ByteReader reader) throws IOException {
    	
    	return getParser().parseFrom(reader);
    }
    
    /**
     * Returns the binary representation of the records.    
     * @return the binary representation of the records.  
     */
    public ReadableBuffer getBuffer() {
		return this.buffer;
	}

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected int computeRecordSetSerializedSize() {
	    return this.buffer.readableBytes();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    protected void writeRecordSetTo(ByteWriter writer) throws IOException {
    	writer.transfer(this.buffer);
    }
}
