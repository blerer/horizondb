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

import io.horizondb.db.utils.TimeUtils;
import io.horizondb.io.Buffer;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.DataChunk;
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.FieldType;
import io.horizondb.model.PartitionId;
import io.horizondb.model.Query;
import io.horizondb.model.Record;
import io.horizondb.model.RecordTypeDefinition;
import io.horizondb.model.TimeRange;
import io.horizondb.model.TimeSeriesDefinition;
import io.horizondb.model.records.TimeSeriesRecord;
import io.horizondb.protocol.Msg;
import io.horizondb.protocol.OpCode;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamedRecordSetTest {

	private TimeSeriesDefinition definition;
	
	@Before
	public void setUp() {
		
		RecordTypeDefinition recordType = RecordTypeDefinition.newBuilder("ExchangeState")
		                                                      .addMillisecondTimestampField("timestamp")
		                                                      .addByteField("status")
		                                                      .build();
		
		DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");

		this.definition = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
		                                      .timeUnit(TimeUnit.NANOSECONDS)
		                                      .addRecordType(recordType)
		                                      .build();
	}
	
	@After
	public void tearDown() {
		
		this.definition = null;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
	public void testNextRecordWithEmptyStream() throws IOException {

		Connection connection = EasyMock.createMock(Connection.class);
		
		Msg<Query> request = createRequest();
		
		Buffer heapBuffer = Buffers.allocate(100);
		heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);
		
		Msg response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.sendRequestAndAwaitResponse(request)).andReturn(response);
		connection.close();
		
		EasyMock.replay(connection);
		
		try (StreamedRecordIterator iterator = new StreamedRecordIterator(this.definition, connection, request)) {
			
			assertFalse(iterator.hasNext());
		}
		
		EasyMock.verify(connection);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
	public void testNextRecordWithOneRecord() throws IOException {

		Connection connection = EasyMock.createMock(Connection.class);
		
		Msg<Query> request = createRequest();
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		Buffer heapBuffer = Buffers.allocate(100);
		writeRecord(heapBuffer, first);
		heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);
		
		Msg response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.sendRequestAndAwaitResponse(request)).andReturn(response);
		connection.close();
		
		EasyMock.replay(connection);
		
		try (StreamedRecordIterator iterator = new StreamedRecordIterator(this.definition, connection, request)) {
			
			assertTrue(iterator.hasNext());
			assertTrue(iterator.hasNext());
			
			Record record = iterator.next();
			
			assertEquals(first.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(first.getByte(2), record.getByte(2));

			assertFalse(iterator.hasNext());
		}
		
		EasyMock.verify(connection);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
	public void testStreamWithTreeRecords() throws Exception {
		
		Connection connection = EasyMock.createMock(Connection.class);
		
		Msg<Query> request = createRequest();
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);
		
		TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
		second.setTimestampInNanos(0, 13000900);
		second.setTimestampInMillis(1, 13);
		second.setByte(2, 3);
		
		TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		Buffer heapBuffer = Buffers.allocate(100);
		writeRecord(heapBuffer, first);
		writeRecord(heapBuffer, second);
		writeRecord(heapBuffer, third);
		heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);
		
		Msg response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.sendRequestAndAwaitResponse(request)).andReturn(response);
		connection.close();
		
		EasyMock.replay(connection);
		
		try (StreamedRecordIterator iterator = new StreamedRecordIterator(this.definition, connection, request)) {
			
			assertTrue(iterator.hasNext());
			
			Record record = iterator.next();
			
			assertEquals(first.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(first.getByte(2), record.getByte(2));
			
			assertTrue(iterator.hasNext());
			
			record = iterator.next();
			
			assertEquals(second.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(second.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(second.getByte(2), record.getByte(2));
			
			assertTrue(iterator.hasNext());
			
			record = iterator.next();
			
			assertEquals(third.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(third.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(third.getByte(2), record.getByte(2));

			assertFalse(iterator.hasNext());
		}
		
		EasyMock.verify(connection);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
	public void testStreamWithTwoChunk() throws Exception {
		
		Connection connection = EasyMock.createMock(Connection.class);
		
		Msg<Query> request = createRequest();
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);
		
		TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
		second.setTimestampInNanos(0, 13000900);
		second.setTimestampInMillis(1, 13);
		second.setByte(2, 3);
		
		TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		Buffer heapBuffer = Buffers.allocate(20);
		writeRecord(heapBuffer, first);
		writeRecord(heapBuffer, second);
		
		Msg response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.sendRequestAndAwaitResponse(request)).andReturn(response);
		
		heapBuffer = Buffers.allocate(20);
		
		writeRecord(heapBuffer, third);
		heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

		response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.awaitResponse()).andReturn(response);
		
		connection.close();
		
		EasyMock.replay(connection);
		
		try (StreamedRecordIterator iterator = new StreamedRecordIterator(this.definition, connection, request)) {
			
			assertTrue(iterator.hasNext());
			
			Record record = iterator.next();
			
			assertEquals(first.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(first.getByte(2), record.getByte(2));
			
			assertTrue(iterator.hasNext());
			
			record = iterator.next();
			
			assertEquals(second.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(second.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(second.getByte(2), record.getByte(2));
			
			assertTrue(iterator.hasNext());
			
			record = iterator.next();
			
			assertEquals(third.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(third.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(third.getByte(2), record.getByte(2));

			assertFalse(iterator.hasNext());
		}
		
		EasyMock.verify(connection);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
	public void testStreamWithOnlyEndOfStreamInSecondChunk() throws Exception {
		
		Connection connection = EasyMock.createMock(Connection.class);
		
		Msg<Query> request = createRequest();
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);
		
		TimeSeriesRecord second = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
		second.setTimestampInNanos(0, 13000900);
		second.setTimestampInMillis(1, 13);
		second.setByte(2, 3);
		
		TimeSeriesRecord third = new TimeSeriesRecord(0, TimeUnit.NANOSECONDS, FieldType.MILLISECONDS_TIMESTAMP, FieldType.BYTE); 
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		Buffer heapBuffer = Buffers.allocate(27);
		writeRecord(heapBuffer, first);
		writeRecord(heapBuffer, second);
		writeRecord(heapBuffer, third);
		
		Msg response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.sendRequestAndAwaitResponse(request)).andReturn(response);
		
		heapBuffer = Buffers.allocate(27);
		
		heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

		response = Msg.newResponseMsg(request.getHeader(), new DataChunk(heapBuffer));
		EasyMock.expect(connection.awaitResponse()).andReturn(response);
		
		connection.close();
		
		EasyMock.replay(connection);
		
		try (StreamedRecordIterator iterator = new StreamedRecordIterator(this.definition, connection, request)) {
			
			assertTrue(iterator.hasNext());
			
			Record record = iterator.next();
			
			assertEquals(first.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(first.getByte(2), record.getByte(2));
			
			assertTrue(iterator.hasNext());
			
			record = iterator.next();
			
			assertEquals(second.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(second.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(second.getByte(2), record.getByte(2));
			
			assertTrue(iterator.hasNext());
			
			record = iterator.next();
			
			assertEquals(third.getTimestampInNanos(0), record.getTimestampInNanos(0));
			assertEquals(third.getTimestampInMillis(1), record.getTimestampInMillis(1));
			assertEquals(third.getByte(2), record.getByte(2));

			assertFalse(iterator.hasNext());
		}
		
		EasyMock.verify(connection);
	}
	
	/**
	 * Writes the specified record in the specified writer.
	 * 
	 * @param writer the writer to write to
	 * @param record the record to write
	 * @throws IOException if a problem occurs while writing the record 
	 */
    private static void writeRecord(ByteWriter writer, TimeSeriesRecord record) throws IOException {
    	
	    writer.writeByte(record.getType());
		VarInts.writeUnsignedInt(writer, record.computeSerializedSize());
		record.writeTo(writer);
    }
    
	/**
	 * Creates the request message.
	 * 
	 * @return the request message.
	 */
    private Msg<Query> createRequest() {
	    long timestamp = TimeUtils.getTime("2013.11.14 11:46:00.000");
		
		TimeRange range = new TimeRange(timestamp, timestamp + 20000);
		
		TimeRange partitionTimeRange = this.definition.getPartitionTimeRange(timestamp);
		
		Query query = new Query(new PartitionId("test", "test", partitionTimeRange.getStart()), range);

		Msg<Query> request = Msg.newRequestMsg(OpCode.QUERY, query);
	    return request;
    }

}
