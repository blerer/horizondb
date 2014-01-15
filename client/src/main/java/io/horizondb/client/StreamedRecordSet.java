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
package io.horizondb.client;

import io.horizondb.io.ReadableBuffer;
import io.horizondb.model.DataChunk;
import io.horizondb.model.Query;
import io.horizondb.model.RecordIterator;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.records.BinaryTimeSeriesRecord;
import io.horizondb.protocol.Msg;

import java.io.IOException;
import java.util.NoSuchElementException;

import static io.horizondb.io.encoding.VarInts.readByte;
import static io.horizondb.io.encoding.VarInts.readUnsignedInt;

/**
 * <code>RecordSet</code> which is received as a stream from the server.
 * 
 * @author Benjamin
 *
 */
final class StreamedRecordIterator implements RecordIterator {

	/**
	 * The connection to the server.
	 */
	private final Connection connection;
	
	/**
	 * The query to be send to the server.
	 */
	private final Msg<Query> query;
	
	/**
	 * The buffer containing the data being processed.
	 */
	private ReadableBuffer buffer;
		
	/**
	 * The binary records.
	 */
	private final BinaryTimeSeriesRecord[] binaryRecords;
	
	/**
	 * The next record to return.
	 */
	private BinaryTimeSeriesRecord next;
	
	/**
	 * <code>true</code> if the next record is ready to be returned.
	 */
	private boolean nextReady;
	
	/**
	 * <code>true</code> if the end of the stream has been reached.
	 */
	private boolean endOfStream;
		
	/**
	 * Creates a new <code>StreamedRecordIterator</code> for the specified query.
	 * 
	 * @param definition the time series definition
	 * @param connection the connection to the server
	 * @param query the query to send to the server
	 */
    public StreamedRecordIterator(TimeSeriesDefinition definition, Connection connection, Msg<Query> query) {
    	
    	this.binaryRecords = definition.newBinaryRecords();
    	this.connection = connection;
    	this.query = query;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() throws IOException {

    	if (this.endOfStream) {
    		return false;
    	}
    	
    	if (this.nextReady) {
    		return true;
    	}
    	
    	return computeNext();
    }

	/**
     * {@inheritDoc}
     */
    @Override
	public BinaryTimeSeriesRecord next() throws IOException {
		
    	if (!hasNext()) {
    		throw new NoSuchElementException();
    	}
    	
    	this.nextReady = false;
    	return this.next;
	}
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() {
    	
    	this.connection.close();
    }	

	private boolean computeNext() throws IOException {

		if (this.buffer == null) {

			Msg<DataChunk> msg = (Msg<DataChunk>) this.connection.sendRequestAndAwaitResponse(this.query);

			this.buffer = msg.getPayload().getBuffer();

		} else if (!this.buffer.isReadable()) {

			Msg<DataChunk> msg = (Msg<DataChunk>) this.connection.awaitResponse();
			this.buffer = msg.getPayload().getBuffer();
		}

		int type = readByte(this.buffer);

		if (type == Msg.END_OF_STREAM_MARKER) {
			
			this.endOfStream = true;
			return false;
		}

		int length = readUnsignedInt(this.buffer);

		this.next = this.binaryRecords[type];
		this.next.fill(this.buffer.slice(length));
		this.nextReady = true;
		
		return true;
    }
}
