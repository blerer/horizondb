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

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.io.encoding.VarInts;
import io.horizondb.model.DataChunk;
import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

import static io.horizondb.io.files.FileUtils.ONE_KB;

/**
 * <code>ChunkedInput</code> that translate into message the records returned by the time series iterator.
 * 
 * @author Benjamin
 * 
 */
public final class ChunkedRecordStream implements ChunkedInput<Msg<DataChunk>> {

    /**
     * The default buffer size.
     */
    private static final int DEFAULT_BUFFER_SIZE = 8 * ONE_KB;

    /**
     * The request header
     */
    private final MsgHeader requestHeader;

    /**
     * The record iterator.
     */
    private final RecordIterator iterator;

    /**
     * The buffer used to writes the records.
     */
    private final Buffer buffer;

    /**
     * The next record to be written.
     */
    private Record next;

    /**
     * <code>true</code> if the end of the input has been reached.
     */
    private boolean endOfInput;

    /**
     * Creates a new <code>ChunkedRecordStream</code> that translate to message the records returned by the specified
     * iterator.
     * 
     * @param requestHeader the request header
     * @param iterator the time series iterator
     * @throws IOException if an I/O problems occurs.
     */
    public ChunkedRecordStream(MsgHeader requestHeader, RecordIterator iterator) throws IOException {

        this(requestHeader, iterator, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new <code>ChunkedRecordStream</code> that translate to message the records returned by the specified
     * iterator.
     * 
     * @param requestHeader the request header
     * @param iterator the time series iterator
     * @param bufferSize the buffer size
     * @throws IOException if an I/O problems occurs.
     */
    public ChunkedRecordStream(MsgHeader requestHeader, RecordIterator iterator, int bufferSize) throws IOException {

        this.requestHeader = requestHeader;
        this.iterator = iterator;
        this.buffer = Buffers.allocate(bufferSize);

        this.next = loadNextRecord();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEndOfInput() throws Exception {
        return this.endOfInput;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        this.iterator.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<DataChunk> readChunk(ChannelHandlerContext ctx) throws Exception {

        this.buffer.clear();

        while (!isEndOfInput()) {

            int writeableBytes = this.buffer.writeableBytes();

            if (this.next == null) {

                if (writeableBytes > 1) {
                    this.endOfInput = true;
                    this.buffer.writeByte(Msg.END_OF_STREAM_MARKER);
                }

                break;
            }

            int serializedSize = this.next.computeSerializedSize();

            if (writeableBytes < 1 + VarInts.computeUnsignedIntSize(serializedSize) + serializedSize) {

                break;
            }

            this.buffer.writeByte(this.next.getType());
            VarInts.writeUnsignedInt(this.buffer, serializedSize);
            this.next.writeTo(this.buffer);

            this.next = loadNextRecord();
        }

        return Msg.newResponseMsg(this.requestHeader, new DataChunk(this.buffer));
    }

    /**
     * Returns the next record or null if there are no more record available.
     * 
     * @throws IOException if an I/O problem occurs while loading the next record.
     */
    private Record loadNextRecord() throws IOException {

        if (this.iterator.hasNext()) {
            return this.iterator.next();
        }

        return null;
    }

}
