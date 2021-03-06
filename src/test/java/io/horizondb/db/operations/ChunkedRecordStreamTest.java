package io.horizondb.db.operations;

import io.horizondb.io.Buffer;
import io.horizondb.io.ByteWriter;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.ResourceIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.protocol.DataChunkPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.schema.FieldType;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 * 
 */
public class ChunkedRecordStreamTest {

    @SuppressWarnings("boxing")
    @Test
    public void testEmptyStream() throws Exception {

        MsgHeader requestHeader = MsgHeader.newRequestHeader(OpCode.HQL_QUERY, 26);
        ResourceIterator<Record> iterator = EasyMock.createMock(ResourceIterator.class);
        EasyMock.expect(iterator.hasNext()).andReturn(false);

        ChannelHandlerContext context = EasyMock.createMock(ChannelHandlerContext.class);

        EasyMock.replay(iterator, context);

        final int bufferSize = 200;

        ChunkedRecordStream input = new ChunkedRecordStream(requestHeader, iterator, bufferSize);

        assertFalse(input.isEndOfInput());

        Msg<DataChunkPayload> msg = input.readChunk(context);

        Buffer heapBuffer = Buffers.allocate(bufferSize);
        heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

        Msg<DataChunkPayload> expected = Msg.newResponseMsg(requestHeader, 
                                                            OpCode.DATA_CHUNK, 
                                                            new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertTrue(input.isEndOfInput());

        EasyMock.verify(iterator, context);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testStreamWithOnlyOneRecord() throws Exception {

        MsgHeader requestHeader = MsgHeader.newRequestHeader(OpCode.HQL_QUERY, 26);
        ResourceIterator<Record> iterator = EasyMock.createMock(ResourceIterator.class);

        TimeSeriesRecord first = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        first.setTimestampInNanos(0, 12000700);
        first.setTimestampInMillis(1, 12);
        first.setByte(2, 3);

        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(first);
        EasyMock.expect(iterator.hasNext()).andReturn(false);

        ChannelHandlerContext context = EasyMock.createMock(ChannelHandlerContext.class);

        EasyMock.replay(iterator, context);

        final int bufferSize = 200;

        ChunkedRecordStream input = new ChunkedRecordStream(requestHeader, iterator, bufferSize);

        assertFalse(input.isEndOfInput());

        Msg<DataChunkPayload> msg = input.readChunk(context);

        Buffer heapBuffer = Buffers.allocate(bufferSize);

        writeRecord(heapBuffer, first);

        heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

        Msg<DataChunkPayload> expected = Msg.newResponseMsg(requestHeader, 
                                                            OpCode.DATA_CHUNK, 
                                                            new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertTrue(input.isEndOfInput());

        EasyMock.verify(iterator, context);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testStreamWithTreeRecords() throws Exception {

        MsgHeader requestHeader = MsgHeader.newRequestHeader(OpCode.HQL_QUERY, 26);
        ResourceIterator<Record> iterator = EasyMock.createMock(ResourceIterator.class);

        TimeSeriesRecord first = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        first.setTimestampInNanos(0, 12000700);
        first.setTimestampInMillis(1, 12);
        first.setByte(2, 3);

        TimeSeriesRecord second = new TimeSeriesRecord(0,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.BYTE);
        second.setTimestampInNanos(0, 13000900);
        second.setTimestampInMillis(1, 13);
        second.setByte(2, 3);

        TimeSeriesRecord third = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        third.setTimestampInNanos(0, 13004400);
        third.setTimestampInMillis(1, 13);
        third.setByte(2, 1);

        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(first);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(second);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(third);
        EasyMock.expect(iterator.hasNext()).andReturn(false);

        ChannelHandlerContext context = EasyMock.createMock(ChannelHandlerContext.class);

        EasyMock.replay(iterator, context);

        final int bufferSize = 200;

        ChunkedRecordStream input = new ChunkedRecordStream(requestHeader, iterator, bufferSize);

        assertFalse(input.isEndOfInput());

        Msg<DataChunkPayload> msg = input.readChunk(context);

        Buffer heapBuffer = Buffers.allocate(bufferSize);
        writeRecord(heapBuffer, first);
        writeRecord(heapBuffer, second);
        writeRecord(heapBuffer, third);
        heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

        Msg<DataChunkPayload> expected = Msg.newResponseMsg(requestHeader, 
                                                            OpCode.DATA_CHUNK, 
                                                            new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertTrue(input.isEndOfInput());

        EasyMock.verify(iterator, context);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testStreamWithTwoChunk() throws Exception {

        MsgHeader requestHeader = MsgHeader.newRequestHeader(OpCode.HQL_QUERY, 26);
        ResourceIterator<Record> iterator = EasyMock.createMock(ResourceIterator.class);

        TimeSeriesRecord first = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        first.setTimestampInNanos(0, 12000700);
        first.setTimestampInMillis(1, 12);
        first.setByte(2, 3);

        TimeSeriesRecord second = new TimeSeriesRecord(0,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.BYTE);
        second.setTimestampInNanos(0, 13000900);
        second.setTimestampInMillis(1, 13);
        second.setByte(2, 3);

        TimeSeriesRecord third = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        third.setTimestampInNanos(0, 13004400);
        third.setTimestampInMillis(1, 13);
        third.setByte(2, 1);

        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(first);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(second);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(third);
        EasyMock.expect(iterator.hasNext()).andReturn(false);

        ChannelHandlerContext context = EasyMock.createMock(ChannelHandlerContext.class);

        EasyMock.replay(iterator, context);

        final int bufferSize = 20;

        ChunkedRecordStream input = new ChunkedRecordStream(requestHeader, iterator, bufferSize);

        assertFalse(input.isEndOfInput());

        Msg<DataChunkPayload> msg = input.readChunk(context);

        Buffer heapBuffer = Buffers.allocate(bufferSize);
        writeRecord(heapBuffer, first);
        writeRecord(heapBuffer, second);

        Msg<DataChunkPayload> expected = Msg.newResponseMsg(requestHeader, 
                                                            OpCode.DATA_CHUNK, 
                                                            new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertFalse(input.isEndOfInput());

        msg = input.readChunk(context);

        heapBuffer.clear();
        writeRecord(heapBuffer, third);
        heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

        expected = Msg.newResponseMsg(requestHeader, 
                                      OpCode.DATA_CHUNK, 
                                      new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertTrue(input.isEndOfInput());

        EasyMock.verify(iterator, context);
    }

    @SuppressWarnings("boxing")
    @Test
    public void testStreamWithOnlyEndOfStreamInSecondChunk() throws Exception {

        MsgHeader requestHeader = MsgHeader.newRequestHeader(OpCode.HQL_QUERY, 26);
        ResourceIterator<Record> iterator = EasyMock.createMock(ResourceIterator.class);

        TimeSeriesRecord first = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        first.setTimestampInNanos(0, 12000700);
        first.setTimestampInMillis(1, 12);
        first.setByte(2, 3);

        TimeSeriesRecord second = new TimeSeriesRecord(0,
                                                       TimeUnit.NANOSECONDS,
                                                       FieldType.MILLISECONDS_TIMESTAMP,
                                                       FieldType.BYTE);
        second.setTimestampInNanos(0, 13000900);
        second.setTimestampInMillis(1, 13);
        second.setByte(2, 3);

        TimeSeriesRecord third = new TimeSeriesRecord(0,
                                                      TimeUnit.NANOSECONDS,
                                                      FieldType.MILLISECONDS_TIMESTAMP,
                                                      FieldType.BYTE);
        third.setTimestampInNanos(0, 13004400);
        third.setTimestampInMillis(1, 13);
        third.setByte(2, 1);

        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(first);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(second);
        EasyMock.expect(iterator.hasNext()).andReturn(true);
        EasyMock.expect(iterator.next()).andReturn(third);
        EasyMock.expect(iterator.hasNext()).andReturn(false);

        ChannelHandlerContext context = EasyMock.createMock(ChannelHandlerContext.class);

        EasyMock.replay(iterator, context);

        final int bufferSize = 27;

        ChunkedRecordStream input = new ChunkedRecordStream(requestHeader, iterator, bufferSize);

        assertFalse(input.isEndOfInput());

        Msg<DataChunkPayload> msg = input.readChunk(context);

        Buffer heapBuffer = Buffers.allocate(bufferSize);
        writeRecord(heapBuffer, first);
        writeRecord(heapBuffer, second);
        writeRecord(heapBuffer, third);

        Msg<DataChunkPayload> expected = Msg.newResponseMsg(requestHeader, 
                                                            OpCode.DATA_CHUNK, 
                                                            new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertFalse(input.isEndOfInput());

        msg = input.readChunk(context);

        heapBuffer.clear();
        heapBuffer.writeByte(Msg.END_OF_STREAM_MARKER);

        expected = Msg.newResponseMsg(requestHeader, 
                                      OpCode.DATA_CHUNK, 
                                      new DataChunkPayload(heapBuffer));

        assertEquals(expected, msg);
        assertTrue(input.isEndOfInput());
        EasyMock.verify(iterator, context);
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
}
