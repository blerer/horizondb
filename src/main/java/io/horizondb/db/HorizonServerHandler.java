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
package io.horizondb.db;

import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.protocol.Msgs;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang.Validate.notNull;

class HorizonServerHandler extends ChannelInboundHandlerAdapter {

    /**
     * The class logger.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The database engine.
     */
    private final DatabaseEngine engine;

    public HorizonServerHandler(DatabaseEngine engine) {

        notNull(engine, "the engine parameter must not be null.");

        this.engine = engine;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object response = this.engine.execute(Buffers.wrap((ByteBuf) msg));
        ctx.channel().writeAndFlush(response);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        this.logger.error("Unexpected exception from downstream.", cause);

        if (ctx.channel().isActive()) {

            ctx.channel().writeAndFlush(Msgs.newErrorMsg(ErrorCodes.INTERNAL_ERROR, cause.getMessage()));
        }
    }
}
