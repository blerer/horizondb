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
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.records.BinaryTimeSeriesRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * <code>RecordIterator</code> for <code>BinaryTimeSeriesRecords</code>
 * 
 * @author Benjamin
 *
 */
public final class BinaryTimeSeriesRecordIterator implements RecordIterator {
	
	/**
	 * The records per type.
	 */
	private final BinaryTimeSeriesRecord[] records;
	
	/**
	 * The ByteReader reader containing the records data.
	 */
	private final ByteReader reader;
	
	
	public BinaryTimeSeriesRecordIterator(TimeSeriesDefinition definition, ByteReader reader) {
		this.records = definition.newBinaryRecords();
		this.reader = reader;
	}
	
	/**	
	 * {@inheritDoc}
	 */
	@Override
    public boolean hasNext() throws IOException {
	    return this.reader.isReadable();
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BinaryTimeSeriesRecord next() throws IOException {
		
		if (!this.reader.isReadable()) {
			
			throw new NoSuchElementException("No more elements are available.");
		}
		
		int type = this.reader.readByte();
		int length = VarInts.readUnsignedInt(this.reader);
		
		ReadableBuffer slice = this.reader.slice(length);

		return this.records[type].fill(slice);
	}

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {
	    
    	if (this.reader instanceof Closeable) {
	        ((Closeable) this.reader).close();
        }
    }
}
