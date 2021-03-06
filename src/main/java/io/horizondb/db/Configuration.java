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
package io.horizondb.db;

import io.horizondb.db.commitlog.CommitLog;
import io.horizondb.db.commitlog.CommitLog.SyncMode;
import io.horizondb.io.compression.CompressionType;
import io.horizondb.io.files.FileUtils;
import io.netty.util.internal.PlatformDependent;

import java.nio.file.Path;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang.Validate;

import static io.horizondb.io.files.FileUtils.ONE_KB;
import static io.horizondb.io.files.FileUtils.ONE_MB;
import static org.apache.commons.lang.Validate.notNull;

/**
 * The database configuration.
 * 
 * @author Benjamin
 */
@Immutable
public final class Configuration {

    /**
     * The port on which the server is listening.
     */
    private final int port;

    /**
     * The data directory.
     */
    private final Path dataDirectory;

    /**
     * The size in bytes of the in memory time series.
     */
    private final int memTimeSeriesSize;

    /**
     * The commit log directory.
     */
    private final Path commitLogDirectory;

    /**
     * The size in bytes of the commit log segments.
     */
    private final long commitLogSegmentSize;

    /**
     * The maximum number of commit log segments.
     */
    private final int maximumNumberOfCommitLogSegments;

    /**
     * Specify how the commit log will sync data to the disk.
     */
    private final CommitLog.SyncMode commitLogSyncMode;
        
    /**
     * The window of time in milliseconds during which the commit log will wait for more writes before flushing 
     * the data to the disk.
     */
    private final long commitLogBatchWindowInMillis;
    
    /**
     * The period of time in milliseconds at which the commit log will flush data to the disk.
     */
    private final long commitLogFlushPeriodInMillis;

    /**
     * The time to wait for shutdown in seconds.
     */
    private final int shutdownWaitingTimeInSeconds;

    /**
     * The maximum size of the database cache.
     */
    private final long databaseCacheMaximumSize;

    /**
     * The maximum size of the time series cache.
     */
    private final long timeSeriesCacheMaximumSize;

    /**
     * The maximum amount of memory that is allowed to be used by the <code>MemTimeSeries</code>.
     */
    private final long maximumMemoryUsageByMemTimeSeries;

    /**
     * The idle time in second after which a <code>MemTimeSeries</code> must be flushed to the disk.
     */
    private final long memTimeSeriesIdleTimeInSecond;

    /**
     * The concurrency level used for the caches.
     */
    private final int cachesConcurrencyLevel;
    
    /**
     * The size in bytes of the partitions blocks.
     */
    private final int blockSizeInBytes;
    
    /**
     * The type of compression used by this time series.
     */
    private final CompressionType compressionType;

    /**
     * Creates a new <code>Builder</code> instance.
     * 
     * @return a new <code>Builder</code> instance.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new <code>Configuration</code> instance from the specified <code>Builder</code>.
     * 
     * @param builder the builder.
     */
    private Configuration(Builder builder) {

        this.port = builder.port;
        this.dataDirectory = builder.dataDirectory;
        this.commitLogDirectory = builder.commitLogDirectory;
        this.commitLogSyncMode = builder.commitLogSyncMode;
        this.commitLogSegmentSize = builder.commitLogSegmentSize;
        this.maximumNumberOfCommitLogSegments = builder.maximumNumberOfCommitLogSegments;
        this.commitLogFlushPeriodInMillis = builder.commitLogFlushPeriodInMillis;
        this.commitLogBatchWindowInMillis = builder.commitLogBatchWindowInMillis;
        this.databaseCacheMaximumSize = builder.databaseCacheMaximumSize;
        this.memTimeSeriesSize = builder.memTimeSeriesSize;
        this.shutdownWaitingTimeInSeconds = builder.shutdownWaitingTimeInSeconds;
        this.maximumMemoryUsageByMemTimeSeries = builder.maximumMemoryUsageByMemTimeSeries;
        this.memTimeSeriesIdleTimeInSecond = builder.memTimeSeriesIdleTimeInSecond;
        this.blockSizeInBytes = builder.blockSizeInBytes;
        this.compressionType = builder.compressionType;
        this.timeSeriesCacheMaximumSize = builder.timeSeriesCacheMaximumSize;
        this.cachesConcurrencyLevel = builder.cachesConcurrencyLevel;
    }

    /**
     * Returns the port on which the server is listening.
     * 
     * @return the port on which the server is listening.
     */
    public int getPort() {

        return this.port;
    }

    /**
     * Returns the directory where the data are stored.
     * 
     * @return the directory where the data are stored.
     */
    public Path getDataDirectory() {

        return this.dataDirectory;
    }

    /**
     * Returns the size in bytes of the commit log segments.
     * 
     * @return the size in bytes of the commit log segments.
     */
    public long getCommitLogSegmentSize() {

        return this.commitLogSegmentSize;
    }

    /**
     * Returns the commit log directory.
     * 
     * @return the commit log directory.
     */
    public Path getCommitLogDirectory() {

        return this.commitLogDirectory;
    }

    /**
     * Returns the mode used by the commit log to sync the data to the disk.  
     *   
     * @return the mode used by the commit log to sync the data to the disk.  
     */
    public CommitLog.SyncMode getCommitLogSyncMode() {
        return this.commitLogSyncMode;
    }

    /**
     * Returns the maximum number of commit log segments.
     * 
     * @return the maximum number of commit log segments.
     */
    public int getMaximumNumberOfCommitLogSegments() {

        return this.maximumNumberOfCommitLogSegments;
    }

    /**
     * Returns the period of time in milliseconds at which the commit log will flush data to the disk.
     * 
     * @return the period of time in milliseconds at which the commit log will flush data to the disk.
     */
    public long getCommitLogFlushPeriodInMillis() {

        return this.commitLogFlushPeriodInMillis;
    }
    
    /**
     * Returns window of time in milliseconds during which the commit log will wait for more writes before flushing 
     * the data to the disk.
     * 
     * @return the window of time in milliseconds during which the commit log will wait for more writes before flushing 
     * the data to the disk
     */
    public long getCommitLogBatchWindowInMillis() {

        return this.commitLogBatchWindowInMillis;
    }

    /**
     * Returns the maximum size of the database cache.
     * 
     * @return the maximum size of the database cache.
     */
    public long getDatabaseCacheMaximumSize() {
        return this.databaseCacheMaximumSize;
    }

    /**
     * Returns the maximum size of the time series cache.
     * 
     * @return the maximum size of the time series cache.
     */
    public long getTimeSeriesCacheMaximumSize() {
        return this.timeSeriesCacheMaximumSize;
    }

    /**
     * Returns the maximum amount of memory that can be used by the time series.
     * 
     * @return the maximum amount of memory that can be used by the time series.
     */
    public long getMaximumMemoryUsageByMemTimeSeries() {
        return this.maximumMemoryUsageByMemTimeSeries;
    }

    /**
     * Returns the life time of the <code>MemTimeSeries</code>. <code>MemTimeSeries</code> that reach that life time
     * will be flushed to the disk.
     * 
     * @return the maximum amount of memory that can be used by the time series.
     */
    public long getMemTimeSeriesIdleTimeInSeconds() {
        return this.memTimeSeriesIdleTimeInSecond;
    }

    /**
     * Returns the size in bytes of the in memory time series.
     * 
     * @return the size in bytes of the in memory time series.
     */
    public int getMemTimeSeriesSize() {
        return this.memTimeSeriesSize;
    }

    /**
     * Returns the time to wait for shutdown in seconds.
     * 
     * @return the time to wait for shutdown in seconds.
     */
    public int getShutdownWaitingTimeInSeconds() {

        return this.shutdownWaitingTimeInSeconds;
    }

    /**
     * Returns the concurrency level to use for the caches.
     * 
     * @return the concurrency level to use for the caches.
     */
    public int getCachesConcurrencyLevel() {
        return this.cachesConcurrencyLevel;
    }
    
    /**
     * Returns the default compression type used by the time series.
     * 
     * @return the default compression type used by the time series
     */
    public CompressionType getCompressionType() {
        return this.compressionType;
    }

    /**
     * Returns the partition block size in bytes.  
     *   
     * @return the partition block size in bytes. 
     */
    public int getBlockSizeInBytes() {
        return this.blockSizeInBytes;
    }

    /**
     * The builder for <code>Configuration</code> instance.
     * 
     * @author benjamin
     */
    public static final class Builder {

        /**
         * The default compression type used by the time series
         */
        private static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.LZ4;

        /**
         * The default size in bytes of the partition blocks.
         */
        private static final int DEFAULT_BLOCK_SIZE_IN_BYTES = 64 * FileUtils.ONE_KB;

        /**
         * The default mode used by the commit log to sync data to the disk.
         */
        private static final SyncMode DEFAULT_COMMITLOG_SYNC_MODE = CommitLog.SyncMode.BATCH;

        /**
         * The default concurrency level for the caches.
         */
        private static final int DEFAULT_CACHES_CONCURRENCY_LEVEL = 4;

        /**
         * The default idle time of a <code>MemTimeSerie</code> before it must be flushed to the disk.
         */
        private static final int DEFAULT_MEMTIMESERIES_IDLE_TIME_IN_SECOND = 2 * 60 * 60;

        /**
         * The default maximum size of the time series cache.
         */
        private static final int DEFAULT_TIMESERIES_CACHE_MAX_SIZE = 200;

        /**
         * The default size of the in memory time series.
         */
        private static final int DEFAULT_MEMTIMESERIES_SIZE = ONE_MB;

        /**
         * The port by default.
         */
        private static final int DEFAULT_PORT = 8553;

        /**
         * The default period of time in millisecond at which the commit-log will flush the data to the disk.
         */
        private static final int DEFAULT_COMMITLOG_FLUSH_PERIOD = 1000;
        
        /**
         * The default commit log batch window.
         */
        private static final int DEFAULT_COMMITLOG_BATCH_WINDOW = 50;

        /**
         * The default size for the database cache.
         */
        private static final long DEFAULT_DATABASE_CACHE_MAX_SIZE = 20;

        /**
         * The port on which the server is listening.
         */
        private int port = DEFAULT_PORT;

        /**
         * The size in bytes of the in memory time series.
         */
        private int memTimeSeriesSize = DEFAULT_MEMTIMESERIES_SIZE;

        /**
         * The data directory.
         */
        private Path dataDirectory;

        /**
         * The commit log directory.
         */
        private Path commitLogDirectory;
        
        /**
         * Specify how the commit log will sync data to the disk.
         */
        private CommitLog.SyncMode commitLogSyncMode = DEFAULT_COMMITLOG_SYNC_MODE;

        /**
         * The maximum number of commit log segments.
         */
        private int maximumNumberOfCommitLogSegments = getDefaultMaximumNumberOfSegments();

        /**
         * The size in bytes of the commit log segments.
         */
        private long commitLogSegmentSize = getDefaultSegmentSize();

        /**
         * The period of time in milliseconds at which the commit log will flush data to the disk.
         */
        private long commitLogFlushPeriodInMillis = DEFAULT_COMMITLOG_FLUSH_PERIOD;
        
        /**
         * The window of time in milliseconds during which the commit log will wait for more writes before flushing 
         * the data to the disk.
         */
        private long commitLogBatchWindowInMillis = DEFAULT_COMMITLOG_BATCH_WINDOW;

        /**
         * The maximum size of the database cache.
         */
        private long databaseCacheMaximumSize = DEFAULT_DATABASE_CACHE_MAX_SIZE;

        /**
         * The time to wait for shutdown in seconds.
         */
        private int shutdownWaitingTimeInSeconds = 30;

        /**
         * The maximum size of the time series cache.
         */
        private long timeSeriesCacheMaximumSize = DEFAULT_TIMESERIES_CACHE_MAX_SIZE;

        /**
         * The maximum amount of memory in bytes that is allowed to be used by the <code>MemTimeSeries</code>.
         */
        private long maximumMemoryUsageByMemTimeSeries = getDefaultMaxMemoryUsageByMemTimeSeries();

        /**
         * The time in second after which an idle <code>MemTimeSeries</code> must be flushed to the disk.
         */
        private long memTimeSeriesIdleTimeInSecond = DEFAULT_MEMTIMESERIES_IDLE_TIME_IN_SECOND;

        /**
         * The concurrency level used for the caches.
         */
        private int cachesConcurrencyLevel = DEFAULT_CACHES_CONCURRENCY_LEVEL;
        
        /**
         * The size in bytes of the partitions blocks.
         */
        private int blockSizeInBytes = DEFAULT_BLOCK_SIZE_IN_BYTES;
                
        /**
         * The default type of compression used by the time series.
         */
        private CompressionType compressionType = DEFAULT_COMPRESSION_TYPE;

        /**
         * Specifies the port on which the database server is listening.
         * 
         * @param port the directory to use to store the commit logs.
         * @return this <code>Builder</code>.
         */
        public Builder port(int port) {

            this.port = port;
            return this;
        }

        /**
         * Specify the size in KB of the in memory time series.
         * 
         * @param memTimeSeriesSizeInKB the size in KB of the in memory time series.
         * @return this <code>builder</code>.
         */
        public Builder memTimeSeriesSizeInKB(int memTimeSeriesSizeInKB) {

            return memTimeSeriesSize(memTimeSeriesSizeInKB * ONE_KB);
        }

        /**
         * Specify the size in bytes of the in memory time series.
         * 
         * @param memTimeSeriesSize the size in bytes of the in memory time series.
         * @return this <code>builder</code>.
         */
        public Builder memTimeSeriesSize(int memTimeSeriesSize) {

            this.memTimeSeriesSize = memTimeSeriesSize;
            return this;
        }

        /**
         * Specifies the data directory.
         * 
         * @param directory the directory to use to store data.
         * @return this <code>Builder</code>.
         */
        public Builder dataDirectory(Path directory) {

            this.dataDirectory = directory;
            return this;
        }

        /**
         * Specifies the commit log directory.
         * 
         * @param directory the directory to use to store the commit logs.
         * @return this <code>Builder</code>.
         */
        public Builder commitLogDirectory(Path directory) {

            this.commitLogDirectory = directory;
            return this;
        }

        /**
         * Specifies mode used by the commit log to sync the data to the disk.
         * 
         * @param commitLogSyncMode the mode used by the commit log to sync the data to the disk
         * @return this <code>Builder</code>.
         */
        public Builder commitLogSyncMode(CommitLog.SyncMode commitLogSyncMode) {

            this.commitLogSyncMode = commitLogSyncMode;
            return this;
        }
        
        /**
         * Specifies the maximum number of commit log segments.
         * 
         * @param maximumNumberOfCommitLogSegments the maximum number of commit log segments.
         * @return this <code>Builder</code>.
         */
        public Builder maximumNumberOfCommitLogSegments(int maximumNumberOfCommitLogSegments) {

            Validate.isTrue(maximumNumberOfCommitLogSegments >= 3,
                            "The maximum number of segments must be greater or equals to 3.");

            this.maximumNumberOfCommitLogSegments = maximumNumberOfCommitLogSegments;
            return this;
        }

        /**
         * Specifies the size in MB of the commit log segments.
         * 
         * @param commitLogSegmentSize the size in MB of the commit log segments.
         * @return this <code>Builder</code>.
         */
        public Builder commitLogSegmentSizeInMB(long commitLogSegmentSizeInMB) {

            return commitLogSegmentSize(commitLogSegmentSizeInMB * ONE_MB);
        }
        
        /**
         * Specifies the window of time in milliseconds during which the commit log will wait for more writes before 
         * flushing the data to the disk.
         * 
         * @param commitLogBatchWindowInMillis the window of time in milliseconds during which the commit log will 
         * wait for more writes before flushing the data to the disk
         * @return this <code>Builder</code>.
         */
        public Builder commitLogBatchWindowInMillis(long commitLogBatchWindowInMillis) {

            Validate.isTrue(commitLogBatchWindowInMillis > 0, "The commit log batch window must be greater than 0.");

            this.commitLogBatchWindowInMillis = commitLogBatchWindowInMillis;
            return this;
        }
        
        /**
         * Specifies the size in bytes of the commit log segments.
         * 
         * @param commitLogSegmentSize the size in bytes of the commit log segments.
         * @return this <code>Builder</code>.
         */
        public Builder commitLogSegmentSize(long commitLogSegmentSize) {

            this.commitLogSegmentSize = commitLogSegmentSize;
            return this;
        }

        /**
         * Specifies the period of time in milliseconds at which the commit log will flush data to the disk.
         * 
         * @param commitLogFlushPeriodInMillis the period of time in milliseconds at which the commit log will flush
         * data to the disk.
         * @return this <code>Builder</code>.
         */
        public Builder commitLogFlushPeriodInMillis(long commitLogFlushPeriodInMillis) {

            Validate.isTrue(commitLogFlushPeriodInMillis > 0, "The commit log flush period must be greater than 0.");

            this.commitLogFlushPeriodInMillis = commitLogFlushPeriodInMillis;
            return this;
        }

        /**
         * Specifies the maximum number of cached databases.
         * 
         * @param databaseCacheMaximumSize the maximum number of cached databases.
         * @return this <code>Builder</code>.
         */
        public Builder databaseCacheMaximumSize(long databaseCacheMaximumSize) {

            Validate.isTrue(databaseCacheMaximumSize > 0, "The database cache maximum size must be greater than 0.");

            this.databaseCacheMaximumSize = databaseCacheMaximumSize;
            return this;
        }

        /**
         * Specifies the time to wait for shutdown in seconds.
         * 
         * @param shutdownWaitingTimeInSeconds Specify the time to wait for shutdown in seconds.
         * @return this <code>Builder</code>.
         */
        public Builder shutdownWaitingTimeInSeconds(int shutdownWaitingTimeInSeconds) {

            this.shutdownWaitingTimeInSeconds = shutdownWaitingTimeInSeconds;
            return this;
        }

        /**
         * Specifies the maximum size of the time series cache.
         * 
         * @param timeSeriesCacheMaximumSize the maximum size of the time series cache.
         * @return this <code>Builder</code>.
         */
        public Builder timeSeriesCacheMaximumSize(long timeSeriesCacheMaximumSize) {

            this.timeSeriesCacheMaximumSize = timeSeriesCacheMaximumSize;
            return this;
        }

        /**
         * Specify the maximum amount of memory in bytes that is allowed to be used by the <code>MemTimeSeries</code>.
         * 
         * @param maximumMemoryUsageByMemTimeSeries the maximum amount of memory in bytes that is allowed to be used by
         * the <code>MemTimeSeries</code>.
         * @return this <code>Builder</code>.
         */
        public Builder maximumMemoryUsageByMemTimeSeries(int maximumMemoryUsageByMemTimeSeries) {

            this.maximumMemoryUsageByMemTimeSeries = maximumMemoryUsageByMemTimeSeries;
            return this;
        }

        /**
         * Specify the maximum amount of memory in MB that is allowed to be used by the <code>MemTimeSeries</code>.
         * 
         * @param maximumMemoryUsageByMemTimeSeriesInMB the maximum amount of memory in bytes that is allowed to be used
         * by the <code>MemTimeSeries</code>.
         * @return this <code>Builder</code>.
         */
        public Builder maximumMemoryUsageByMemTimeSeriesInMB(int maximumMemoryUsageByMemTimeSeriesInMB) {

            this.maximumMemoryUsageByMemTimeSeries = maximumMemoryUsageByMemTimeSeriesInMB * ONE_MB;
            return this;
        }

        /**
         * Specify the time in second after which a <code>MemTimeSeries</code> must be flushed to the disk.
         * 
         * @param memTimeSeriesIdleTimeInSecond the time in second after which a <code>MemTimeSeries</code> must be
         * flushed to the disk.
         * @return this <code>Builder</code>.
         */
        public Builder memTimeSeriesIdleTimeInSecond(int memTimeSeriesIdleTimeInSecond) {

            this.memTimeSeriesIdleTimeInSecond = memTimeSeriesIdleTimeInSecond;
            return this;
        }

        /**
         * Specify the concurrency level for the caches.
         * 
         * @param cachesConcurrencyLevel the concurrency level for the caches.
         * @return this <code>Builder</code>.
         */
        public Builder cachesConcurrencyLevel(int cachesConcurrencyLevel) {

            this.cachesConcurrencyLevel = cachesConcurrencyLevel;
            return this;
        }

        /**
         * Specify the default partition block size in bytes.
         * 
         * @param blockSizeInBytes the default partition block size in bytes.
         * @return this <code>Builder</code>.
         */
        public Builder blockSizeInBytes(int blockSizeInBytes) {

            this.blockSizeInBytes = blockSizeInBytes;
            return this;
        }
        
        /**
         * Sets the default type of compression used by the time series.
         * 
         * @param compressionType the default type of compression used by the time series.
         * @return this <code>Builder</code>.
         */
        public Builder compressionType(CompressionType compressionType) {

            notNull(compressionType, "the compressionType parameter must not be null.");

            this.compressionType = compressionType;
            return this;
        }
        
        /**
         * Builds a new <code>Configuration</code> instance.
         * 
         * @return a new <code>Configuration</code> instance.
         */
        public Configuration build() {

            return new Configuration(this);
        }

        /**
         * Must not be instantiated.
         */
        private Builder() {
        }
        
        /**
         * Returns the default size for the commit log segments.
         * 
         * @return the default size for the commit log segments.
         */
        private static int getDefaultSegmentSize() {
            
            if (PlatformDependent.bitMode() == 32) {
                
                return 8 * ONE_MB;
            }
            
            return 32 * ONE_MB;
        }
        
        /**
         * Returns the default maximum number of commit log segments.
         * 
         * @return the default maximum number of commit log segments.
         */
        private static int getDefaultMaximumNumberOfSegments() {
            
            if (PlatformDependent.bitMode() == 32) {
                
                return 4;
            }
            
            return 32;
        }
        
        /**
         * Return the default amount of memory that can be used by all the memory time series.
         * 
         * @return the default amount of memory that can be used by all the memory time series.
         */
        private static long getDefaultMaxMemoryUsageByMemTimeSeries() {
            
            long maxMemory = Runtime.getRuntime().maxMemory();
            
            return maxMemory / 4;
        }
    }
}
