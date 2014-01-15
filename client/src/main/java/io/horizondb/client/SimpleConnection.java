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

import io.horizondb.model.Error;
import io.horizondb.protocol.Msg;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Benjamin
 *
 */
 class SimpleConnection implements Closeable, Connection {

	 /**
	  * The client configuration.
	  */
	 private final ClientConfiguration configuration;
	 
	 /**
	  * The channel.
	  */
	 private final Channel channel;
	 
	 /**
	  * The queue used to store the response messages.
	  */
	 private final BlockingQueue<Msg<?>> queue;
	 
	/**
	 * @param channel 
	 * 
	 */
	public SimpleConnection(ClientConfiguration configuration, Channel channel) {
		
		this.configuration = configuration;
		this.channel = channel;
		this.queue = ((ClientHandler) this.channel.pipeline().last()).getQueue();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
    public Msg<?> sendRequestAndAwaitResponse(Msg<?> request) {
		
		sendRequest(request);
		return awaitResponse();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
    public void sendRequest(Msg<?> request) {
	    
    	this.queue.clear();
		
		ChannelFuture future = this.channel.writeAndFlush(request);
		future.awaitUninterruptibly();
		
		if (!future.isSuccess()) {
			
			throw new HorizonDBException("",  future.cause());
		}
    }

	/**
	 * {@inheritDoc}
	 */
	@Override
    public Msg<?> awaitResponse() {
	    try {
	        
			Msg<?> response = this.queue.poll(this.configuration.getQueryTimeoutInSeconds(), TimeUnit.SECONDS);
			
			if (response == null) {
				
				throw new QueryTimeoutException("No response has been received for more than " 
						+ this.configuration.getQueryTimeoutInSeconds() + " seconds.");
			}
			
			if (!response.getHeader().isSuccess()) {
				
				throw new HorizonDBException((Error) response.getPayload());
			}
			
			return response;
	        
        } catch (InterruptedException e) {
	                	
        	Thread.currentThread().interrupt();
        	throw new HorizonDBException("", e);
        }
    }
	
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() {
    	
    	this.channel.disconnect();
    }
}
