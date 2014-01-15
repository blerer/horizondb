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

import io.horizondb.io.serialization.Serializable;
import io.horizondb.protocol.Msg;
import io.horizondb.protocol.MsgHeader;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;

/**
 * @author Benjamin
 *
 */
class ConnectionManager implements Closeable {

	private final ClientConfiguration configuration;

	private Bootstrap bootstrap;
	
	/**
	 * 
	 */
	public ConnectionManager(ClientConfiguration configuration) {

		this.configuration = configuration;
		this.bootstrap = new Bootstrap().group(new NioEventLoopGroup())
		                                .channel(NioSocketChannel.class)
		                                .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
		                                .handler(new ChannelInitializer<SocketChannel>() {

			                                @Override
			                                public void initChannel(SocketChannel ch) throws Exception {
			                                	
			                			    	int adjustment = MsgHeader.HEADER_SIZE 
			                			    			- (MsgHeader.LENGTH_FIELD_OFFSET + MsgHeader.LENGTH_FIELD_LENGTH);
			                                	
				                                ch.pipeline()
				                                  .addLast("encoder", new MsgToByteEncoder())
				                                  .addLast(new LengthFieldBasedFrameDecoder(ByteOrder.LITTLE_ENDIAN,
				                                                Integer.MAX_VALUE,
				                                                MsgHeader.LENGTH_FIELD_OFFSET,
				                                                MsgHeader.LENGTH_FIELD_LENGTH,
				                                                adjustment,
				                                                0,
				                                                true))
				                                  .addLast("client", new ClientHandler())
				                                  ;
			                                }
		                                });
	}	
	
	public SimpleConnection getConnection() {
		
		return openConnection(this.configuration.getHostAddress());
	}
	
	private SimpleConnection openConnection(InetSocketAddress address) {

		Channel channel = this.bootstrap.clone()
		                                .connect(address)
		                                .syncUninterruptibly()
		                                .channel();

		return new SimpleConnection(this.configuration, channel);
	}

	/**
	 * Sends the specified message.
	 * 
	 * @param msg the message to send.
	 * @return the response returned by the server.
	 */
    public <T extends Serializable> Msg<T> send(Msg<?> msg) {
	    
    	try (SimpleConnection simpleConnection = getConnection()) {
			return (Msg<T>) simpleConnection.sendRequestAndAwaitResponse(msg);
		}
    }
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() {
    	
    	this.bootstrap.group().shutdownGracefully();
    }
}
