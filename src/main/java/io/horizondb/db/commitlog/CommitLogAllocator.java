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
package io.horizondb.db.commitlog;

import io.horizondb.db.AbstractComponent;
import io.horizondb.db.Configuration;
import io.horizondb.db.StorageEngine;
import io.horizondb.db.metrics.PrefixFilter;
import io.horizondb.db.metrics.ThreadPoolExecutorMetrics;
import io.horizondb.db.util.concurrent.ExecutorsUtils;
import io.horizondb.db.util.concurrent.NamedThreadFactory;
import io.horizondb.db.util.concurrent.SyncTask;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.MetricRegistry;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Performs the preallocation and recycling of commit log segment files.
 * 
 * @author Benjamin
 */
@ThreadSafe
final class CommitLogAllocator extends AbstractComponent {

    /**
     * The database configuration.
     */
    private final Configuration configuration;

    /**
     * The database engine.
     */
    private final StorageEngine databaseEngine;

    /**
     * Segments ready to be used
     */
    private final BlockingQueue<CommitLogSegment> availableSegments = new LinkedBlockingQueue<>();

    /**
     * Active segments, containing non flushed data
     */
    private final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue<>();

    /**
     * The executor running the allocation tasks.
     */
    private ExecutorService executor;

    /**
     * Creates a new <code>CommitLogAllocator</code> instance that will use the specified configuration.
     * 
     * @param configuration the database configuration.
     * @param databaseEngine the database engine.
     */
    public CommitLogAllocator(Configuration configuration, StorageEngine databaseEngine) {

        this.databaseEngine = databaseEngine;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(MetricRegistry registry) {
        registry.registerAll(new ThreadPoolExecutorMetrics(name(getName(), "executor"),
                                                           (ThreadPoolExecutor) this.executor));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unregister(MetricRegistry registry) {
        registry.removeMatching(new PrefixFilter(getName()));
    }

    /**
     * Fetches the next writable segment file.
     * 
     * @return the next writable segment
     * @throws InterruptedException if the tread is interrupted while waiting for an available segment.
     */
    public CommitLogSegment fetchSegment() throws InterruptedException {

        checkRunning();

        CommitLogSegment next = this.availableSegments.take();

        this.activeSegments.add(next);

        this.logger.debug("CommitLogSegment {} has been added to the active segments.", Long.valueOf(next.getId()));

        this.executor.execute(new AllocationTask());

        return next;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doStart() throws IOException {

        ThreadFactory threadFactory = new NamedThreadFactory(getName() + "-SegmentAllocator");
        this.executor = Executors.newFixedThreadPool(1, threadFactory);

        loadActiveSegmentsFromDisk();
        replayActiveSegments();

        this.executor.execute(new AllocationTask());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doShutdown() throws InterruptedException {

        try {

            ExecutorsUtils.shutdownAndAwaitForTermination(this.executor,
                                                          this.configuration.getShutdownWaitingTimeInSeconds());

        } finally {

            closeActiveSegments();
            closeAvailableSegments();
        }
    }

    /**
     * Returns the active segments.
     * 
     * @return the active segments.
     */
    Iterable<CommitLogSegment> getActiveSegments() {

        return this.activeSegments;
    }

    /**
     * Blocks until all the allocation task previously submitted have been completed.
     * <p>
     * This method is implemented for testing purpose.
     * </p>
     * 
     * @throws Exception if a problem occurs while synchronizing.
     */
    void sync() throws Exception {

        this.executor.submit(new SyncTask()).get();
    }

    /**
     * Allocates a new segment by either creating a new one or by recycling the oldest one.
     * 
     * @throws InterruptedException if the current thread has been interrupted.
     */
    private void allocateNextSegment() throws InterruptedException {

        if (hasReachMaximumNumberOfSegments()) {

            recycleSegment();

        } else {

            createNewSegment();
        }
    }

    /**
     * Checks if the maximum number of segments has been reached.
     * 
     * @return <code>true</code> if the maximum number of segments has been reached, <code>false</code> otherwise.
     */
    private boolean hasReachMaximumNumberOfSegments() {

        return this.activeSegments.size() == this.configuration.getMaximumNumberOfCommitLogSegments();
    }

    /**
     * Creates a new commit log segment.
     * 
     * @throws InterruptedException
     */
    private void createNewSegment() throws InterruptedException {

        try {

            this.availableSegments.add(CommitLogSegment.freshSegment(this.configuration));

        } catch (IOException e) {

            this.logger.error("An error has occured while creating a new segment.", e);
        }
    }

    /**
     * Recycles the oldest active segment forcing the table manager to flush some data to the disk if some data of the
     * segment have not been flushed to the disk yet.
     * 
     * @throws InterruptedException if the current thread was interrupted while waiting for the table manager to flush
     * some data.
     */
    private void recycleSegment() throws InterruptedException {

        CommitLogSegment old = this.activeSegments.poll();

        try {

            old.flush();
            this.databaseEngine.forceFlush(old.getId());
            this.availableSegments.add(CommitLogSegment.recycleSegment(old));

        } catch (IOException e) {

            this.logger.error("An error has occured while recycling the segment " + old.getPath() + " .", e);
        }
    }

    /**
     * Loads the active segments from the disk.
     * 
     * @throws IOException if a problem occurs while retrieving the segments from the disk.
     */
    private void loadActiveSegmentsFromDisk() throws IOException {

        this.activeSegments.addAll(loadSegmentsFromDisk());
    }

    /**
     * Replays the active segments in order.
     * 
     * @throws IOException if a problem occurs while replaying the segments.
     */
    private void replayActiveSegments() throws IOException {

        for (CommitLogSegment segment : this.activeSegments) {

            this.logger.info("Replaying segment: " + segment.getPath().getFileName());

            int count = segment.replay(this.databaseEngine);

            this.logger.info("{} messages have been replayed from segment: {}", 
                             Integer.valueOf(count), 
                             segment.getPath().getFileName());
        }
    }

    /**
     * Loads the segments found in the commit log directory.
     * 
     * @return the segments found in the commit log directory in order.
     * @throws IOException if a problem occurs while loading the segments.
     */
    private List<CommitLogSegment> loadSegmentsFromDisk() throws IOException {

        Path logDirectory = this.configuration.getCommitLogDirectory();

        if (!Files.exists(logDirectory)) {

            this.logger.debug("Creating commitlog directory: {}.", logDirectory);
            Files.createDirectory(logDirectory);

            return Collections.emptyList();
        }

        this.logger.debug("Loading commitlog segments from the directory: {}", logDirectory);

        final List<CommitLogSegment> segments = new ArrayList<>();

        Files.walkFileTree(logDirectory, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

                if (CommitLogSegment.isCommitLogSegment(file)) {

                    try {

                        segments.add(CommitLogSegment.loadFromFile(CommitLogAllocator.this.configuration, file));

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                return FileVisitResult.CONTINUE;
            }
        });

        Collections.sort(segments);
        return segments;
    }

    /**
     * Closes the available segments.
     */
    private void closeAvailableSegments() {

        closeSegments(this.availableSegments);
    }

    /**
     * Closes the active segments.
     */
    private void closeActiveSegments() {

        closeSegments(this.activeSegments);
    }

    /**
     * Closes the specified segments.
     * 
     * @param segments the segments to close.
     */
    private static void closeSegments(Iterable<CommitLogSegment> segments) {

        for (CommitLogSegment segment : segments) {
            segment.close();
        }
    }

    /**
     * <code>Runnable</code> that will try to allocate a new segment for future use.
     * 
     */
    private class AllocationTask implements Runnable {

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {

            try {

                allocateNextSegment();

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
            }
        }
    }
}
