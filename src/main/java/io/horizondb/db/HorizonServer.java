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

import io.horizondb.db.util.concurrent.NamedThreadFactory;
import io.horizondb.model.protocol.MsgHeader;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.concurrent.ThreadFactory;

import com.codahale.metrics.MetricRegistry;

import static org.apache.commons.lang.Validate.notNull;

public class HorizonServer extends AbstractComponent {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The database engine.
     */
    private final DatabaseEngine engine;

    private EventLoopGroup acceptGroup;

    private EventLoopGroup connectGroup;

    private EventExecutorGroup executor;

    public HorizonServer(Configuration configuration) {

        notNull(configuration, "the configuration parameter must not be null.");

        this.configuration = configuration;
        this.engine = new HqlConverter(new DefaultDatabaseEngine(configuration));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        this.engine.register(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        this.engine.unregister(registry);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws InterruptedException
     */
    @Override
    protected void doStart() throws IOException, InterruptedException {

        this.engine.start();

        ThreadFactory acceptFactory = new NamedThreadFactory("accept");
        ThreadFactory connectFactory = new NamedThreadFactory("connect");
        ThreadFactory workerFactory = new NamedThreadFactory("worker");

        this.executor = new DefaultEventExecutorGroup(8, workerFactory);

        this.acceptGroup = new NioEventLoopGroup(1, acceptFactory);
        this.connectGroup = new NioEventLoopGroup(1, connectFactory);

        ServerBootstrap boot = new ServerBootstrap();
        boot.group(this.acceptGroup, this.connectGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, Integer.valueOf(100))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {

                    int adjustment = MsgHeader.HEADER_SIZE
                            - (MsgHeader.LENGTH_FIELD_OFFSET + MsgHeader.LENGTH_FIELD_LENGTH);

                    ch.pipeline()

                      .addLast(new LengthFieldBasedFrameDecoder(ByteOrder.LITTLE_ENDIAN,
                                                                Integer.MAX_VALUE,
                                                                MsgHeader.LENGTH_FIELD_OFFSET,
                                                                MsgHeader.LENGTH_FIELD_LENGTH,
                                                                adjustment,
                                                                0,
                                                                true))
                      .addLast("encoder", new MsgToByteEncoder())
                      .addLast(HorizonServer.this.executor, "chunkedWriter", new ChunkedWriteHandler())
                      .addLast(HorizonServer.this.executor, new HorizonServerHandler(HorizonServer.this.engine));
                }
            });

        boot.bind(this.configuration.getPort()).awaitUninterruptibly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        this.acceptGroup.shutdownGracefully();
        this.executor.shutdownGracefully();
        this.connectGroup.shutdownGracefully();

        this.engine.shutdown();
    }
}
