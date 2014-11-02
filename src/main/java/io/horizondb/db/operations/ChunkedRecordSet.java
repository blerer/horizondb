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
package io.horizondb.db.operations;

import io.horizondb.model.protocol.DataHeaderPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.MsgHeader;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.Payload;
import io.horizondb.model.schema.RecordSetDefinition;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

/**
 * @author Benjamin
 *
 */
public class ChunkedRecordSet implements ChunkedInput<Msg<?>> {

    /**
     * The request header
     */
    private final MsgHeader requestHeader;
    
    /**
     * The record set definition
     */
    private final RecordSetDefinition definition;
    
    /**
     * The data chunks
     */
    private final ChunkedRecordStream recordStream;
    
    /**
     * <code>true</code> if this is the start of the input.
     */
    private boolean isStartOfInput = true;
        
    /**
     * Creates a new <code>ChunkedRecordSet</code>.
     * 
     * @param requestHeader the request header
     * @param definition the time series definition for which data is returned
     * @param recordStream the record stream
     */
    public ChunkedRecordSet(MsgHeader requestHeader, RecordSetDefinition definition, ChunkedRecordStream recordStream) {

        this.requestHeader = requestHeader;
        this.definition = definition;
        this.recordStream = recordStream;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        this.recordStream.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEndOfInput() throws Exception {
        return this.recordStream.isEndOfInput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Msg<?> readChunk(ChannelHandlerContext arg0) throws Exception {
        
        if (this.isStartOfInput) {
            
            this.isStartOfInput = false;
            
            Payload payload = new DataHeaderPayload(this.definition);
            return Msg.newResponseMsg(this.requestHeader, OpCode.DATA_HEADER, payload);            
        }
        
        return this.recordStream.readChunk(arg0);
    }
}
