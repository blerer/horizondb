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

import java.nio.ByteOrder;

import io.horizondb.io.buffers.Buffers;
import io.horizondb.protocol.Msg;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Encoder for <code>Msg</code>.
 * 
 * @author Benjamin
 *
 */
final class MsgToByteEncoder extends MessageToByteEncoder<Msg<?>> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void encode(ChannelHandlerContext ctx, Msg<?> msg, ByteBuf out) throws Exception {
		
		out.writerIndex(Buffers.wrap(out.capacity(msg.computeSerializedSize()))
		                       .order(ByteOrder.LITTLE_ENDIAN)
				               .writeObject(msg)
				               .writerIndex());
	}
}
