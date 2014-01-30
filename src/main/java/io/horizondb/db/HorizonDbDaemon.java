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

import io.horizondb.db.util.concurrent.LoggingUncaughtExceptionHandler;
import io.horizondb.model.ErrorCodes;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;

import static org.apache.commons.lang.SystemUtils.JAVA_VERSION;
import static org.apache.commons.lang.SystemUtils.JAVA_VM_NAME;

/**
 * The HorizonDB daemon used to run the database.
 * 
 * @author Benjamin
 * 
 */
public final class HorizonDbDaemon {

    /**
     * The class logger.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Creates the container for the database metrics.
     */
    private final MetricRegistry registry = new MetricRegistry();

    /**
     * The metric reporter.
     */
    private ScheduledReporter reporter;

    /**
     * The daemon instance.
     */
    private static final HorizonDbDaemon INSTANCE = new HorizonDbDaemon();

    /**
     * The horizon server.
     */
    private HorizonServer server;

    public static void main(String[] args) {

        INSTANCE.start();
    }

    public static void stop(String[] args) {

        INSTANCE.stop();
    }

    /**
     * Starts the database.
     */
    private void start() {

        try {

            Thread.setDefaultUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler());

            printJvmInfo();

            Configuration configuration = loadConfiguration();

            startServer(configuration);
            startMonitoring();

        } catch (Exception e) {

            this.logger.error("Cannot start the server due to the following error:", e);
            System.exit(ErrorCodes.INTERNAL_ERROR);
        }
    }

    /**
     * Prints the JVM information to the log file.
     */
    @SuppressWarnings("boxing")
    private void printJvmInfo() {

        this.logger.info("JVM vendor/version: {}/{}", JAVA_VM_NAME, JAVA_VERSION);

        if (!JAVA_VM_NAME.contains("HotSpot")) {
            this.logger.warn("Non-Oracle JVM detected.  Some features, such as immediate unmap, may not work");
        }

        this.logger.info("Heap size: {}/{}", Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());
        this.logger.info("Classpath: {}", System.getProperty("java.class.path"));
    }

    /**
     * Stops the database.
     */
    private void stop() {

        this.logger.info("Shutting down HorizonDB");

        try {

            stopMonitoring();
            stopServer();

        } catch (InterruptedException e) {

            this.logger.error("", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the database server.
     * 
     * @param configuration the database configuration
     * @throws InterruptedException f the thread has been interrupted
     * @throws IOException if an I/O problem occurs
     */
    private void startServer(Configuration configuration) throws InterruptedException, IOException {

        this.server = new HorizonServer(configuration);
        this.server.start();
    }

    /**
     * Stops the database server.
     * 
     * @throws InterruptedException if the thread has been interrupted.
     */
    private void stopServer() throws InterruptedException {

        this.server.shutdown();
    }

    /**
     * Starts the monitoring of the database.
     */
    private void startMonitoring() {

        this.reporter = Slf4jReporter.forRegistry(this.registry)
                                     .outputTo(LoggerFactory.getLogger("io.horizondb.metrics"))
                                     .convertRatesTo(TimeUnit.SECONDS)
                                     .convertDurationsTo(TimeUnit.MILLISECONDS)
                                     .build();

        this.reporter.start(1, TimeUnit.MINUTES);

        this.server.register(this.registry);
    }

    /**
     * Stop the monitoring of the database.
     */
    private void stopMonitoring() {

        this.reporter.stop();
        this.server.unregister(this.registry);
    }

    /**
     * Loads the configuration from the classpath.
     * 
     * @return the loaded configuration
     * @throws IOException if an I/O problem occurs while loading the configuration
     * @throws HorizonDBException if the configuration is invalid
     */
    private Configuration loadConfiguration() throws IOException, HorizonDBException {

        ConfigurationLoader loader = new PropertiesFileConfigurationLoader();

        try (InputStream input = this.getClass().getResourceAsStream("horizondb-config.properties")) {

            return loader.loadConfigurationFrom(input);
        }
    }

}
