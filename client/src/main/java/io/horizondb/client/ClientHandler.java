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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.protocol.Msg;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ClientHandler extends ChannelInboundHandlerAdapter {

	/**
	 * The queue used to store the server response.
	 */
	private final BlockingQueue<Msg<?>> queue = new LinkedBlockingQueue<Msg<?>>(); 
	
		
    public BlockingQueue<Msg<?>> getQueue() {
		return this.queue;
	}

	@Override
    public void channelActive(ChannelHandlerContext ctx) {
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

    	Buffer buffer = Buffers.wrap((ByteBuf) msg);
    	
    	Msg<?> message = Msg.parseFrom(buffer);
    	
    	this.queue.add(message);
    
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    	cause.printStackTrace();
    }
}
