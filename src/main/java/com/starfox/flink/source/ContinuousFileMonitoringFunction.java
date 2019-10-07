/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starfox.flink.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.*;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * This is the single (non-parallel) monitoring task which takes a {@link FileInputFormat}
 * and, depending on the {@link FileProcessingMode} and the {@link FilePathFilter}, it is responsible for:
 *
 * <ol>
 *     <li>Monitoring a user-provided path.</li>
 *     <li>Deciding which files should be further read and processed.</li>
 *     <li>Creating the {@link FileInputSplit splits} corresponding to those files.</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link ContinuousFileReaderOperator}
 * which can have parallelism greater than one.
 *
 * <p><b>IMPORTANT NOTE: </b> Splits are forwarded downstream for reading in ascending modification time order,
 * based on the modification time of the files they belong to.
 */
@Internal
public class ContinuousFileMonitoringFunction<OUT>
        extends RichSourceFunction<TimestampedFileInputSplit> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileMonitoringFunction.class);

    /**
     * The minimum interval allowed between consecutive path scans.
     *
     * <p><b>NOTE:</b> Only applicable to the {@code PROCESS_CONTINUOUSLY} mode.
     */
    public static final long MIN_MONITORING_INTERVAL = 1L;

    /**
     * The minimum offset allowed to re-scan the directory for out-of-order files.
     *
     * <p><b>NOTE:</b> Only applicable to the {@code PROCESS_CONTINUOUSLY} mode.
     */
    public static final long MIN_READ_CONSISTENCY_OFFSET_INTERVAL = 0L;

    /** The path to monitor. */
    private final String path;

    /** The parallelism of the downstream readers. */
    private final int readerParallelism;

    /** The {@link FileInputFormat} to be read. */
    private final FileInputFormat<OUT> format;

    /** The interval between consecutive path scans. */
    private final long interval;

    /** Which new data to process (see {@link FileProcessingMode}. */
    private final FileProcessingMode watchType;

    /** The offset interval back from the latest file modification timestamp to scan for our-of-order files.
     *  Valid value for this is from 0 to Long.MAX_VALUE.
     *
     *  <p><b>NOTE: </b>: Files with (modTime > Long.MIN_VALUE + readConsistencyOffset) will NOT be read.
     */
    private final long readConsistencyOffset;

    /** The current modification time watermark. */
    private volatile long globalModificationTime = Long.MIN_VALUE;

    /** The maximum file modification time seen so far. */
    private volatile long maxProcessedTime = Long.MIN_VALUE;

    /** The list of processed files having modification time within the period from globalModificationTime
     *  to maxProcessedTime in the form of a Map&lt;filePath, lastModificationTime&gt;. */
    private volatile Map<String, Long> processedFiles;

    /** The {@link FileInputFormat} to be read. */
    private final DirectoriesPartitioner directoriesPartitioner;

    private transient long maxCurrentTimestamp;
    private transient long nearestFutureTimestamp;

    private long windowBegin;
    private long windowEnd;
    private long prevWindowEnd;
    // End of directories-partitioner related params

    private transient Object checkpointLock;

    private volatile boolean isRunning = true;

    private transient ListState<Long> checkpointedState;

    private transient ListState<Map<String, Long>> checkpointedStateProcessedFilesList;

    public ContinuousFileMonitoringFunction(
            FileInputFormat<OUT> format,
            FileProcessingMode watchType,
            int readerParallelism,
            long interval) {
        this(format, watchType, readerParallelism, interval, 0L);
    }

    public ContinuousFileMonitoringFunction(
            FileInputFormat<OUT> format,
            FileProcessingMode watchType,
            int readerParallelism,
            long interval,
            long readConsistencyOffset) {

        Preconditions.checkArgument(
                watchType == FileProcessingMode.PROCESS_ONCE || interval >= MIN_MONITORING_INTERVAL,
                "The specified monitoring interval (" + interval + " ms) is smaller than the minimum " +
                        "allowed one (" + MIN_MONITORING_INTERVAL + " ms)."
        );

        Preconditions.checkArgument(
                format.getFilePaths().length == 1,
                "FileInputFormats with multiple paths are not supported yet.");

        Preconditions.checkArgument(
                watchType == FileProcessingMode.PROCESS_ONCE || readConsistencyOffset >= MIN_READ_CONSISTENCY_OFFSET_INTERVAL,
                "The specified read-consistency offset (" + readConsistencyOffset +
                        " ms) is smaller than the minimum allowed one (" +
                        MIN_READ_CONSISTENCY_OFFSET_INTERVAL + " ms)."
        );

        this.format = Preconditions.checkNotNull(format, "Unspecified File Input Format.");
        this.path = Preconditions.checkNotNull(format.getFilePaths()[0].toString(), "Unspecified Path.");

        this.interval = interval;
        this.watchType = watchType;
        this.readerParallelism = Math.max(readerParallelism, 1);
        this.readConsistencyOffset = readConsistencyOffset;
        this.globalModificationTime = Long.MIN_VALUE;
        this.maxProcessedTime = Long.MIN_VALUE + this.readConsistencyOffset;
        this.processedFiles = new HashMap<>();

        if (format instanceof DirectoriesPartitioner) {
            directoriesPartitioner = (DirectoriesPartitioner)format;
        } else {
            directoriesPartitioner = null;
        }
        this.maxCurrentTimestamp = Long.MIN_VALUE;
        this.nearestFutureTimestamp = Long.MIN_VALUE;
        this.prevWindowEnd = Long.MIN_VALUE;
        this.windowEnd = Long.MIN_VALUE;
        this.windowBegin = Long.MIN_VALUE;
    }

    @VisibleForTesting
    public long getGlobalModificationTime() {
        return this.globalModificationTime;
    }

    @VisibleForTesting
    public long getMaxProcessedTime() {
        return this.maxProcessedTime;
    }

    @VisibleForTesting
    public Map<String, Long> getProccessedFiles() {
        return this.processedFiles;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        Preconditions.checkState(this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "file-monitoring-state",
                        LongSerializer.INSTANCE
                )
        );
        this.checkpointedStateProcessedFilesList = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "file-monitoring-state-processed-files-list",
                        new MapSerializer<>(StringSerializer.INSTANCE, LongSerializer.INSTANCE)
                )
        );

        if (context.isRestored()) {
            LOG.info("Restoring state for the {}.", getClass().getSimpleName());

            List<Long> retrievedStates = new ArrayList<>();
            for (Long entry : this.checkpointedState.get()) {
                retrievedStates.add(entry);
            }
            List<Map<String, Long>> retrievedStates2 = new ArrayList<>();
            for (Map<String, Long> entry : this.checkpointedStateProcessedFilesList.get()) {
                retrievedStates2.add(entry);
            }

            // given that the parallelism of the function is 1, we can only have 1 or 0 retrieved items.
            // the 0 is for the case that we are migrating from a previous Flink version.

            Preconditions.checkArgument(retrievedStates.size() <= 1 && retrievedStates2.size() <= 1,
                    getClass().getSimpleName() + " retrieved invalid state.");

            if (retrievedStates.size() == 1 && globalModificationTime != Long.MIN_VALUE) {
                // this is the case where we have both legacy and new state.
                // The two should be mutually exclusive for the operator, thus we throw the exception.

                throw new IllegalArgumentException(
                        "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

            } else if (retrievedStates.size() == 1) {
                this.globalModificationTime = retrievedStates.get(0);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} retrieved a global mod time of {}.",
                            getClass().getSimpleName(), globalModificationTime);
                }
                if (retrievedStates2.size() == 1 && processedFiles.size() != 0) {
                    // this is the case where we have both legacy and new state.
                    // The two should be mutually exclusive for the operator, thus we throw the exception.
                    throw new IllegalArgumentException(
                            "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");
                } else if (retrievedStates2.size() == 1) {
                    this.processedFiles = retrievedStates2.get(0);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{} retrieved a list of {} processed files.",
                                getClass().getSimpleName(), processedFiles.size());
                    }
                }
                // Infer new maxProcessedTime from the list of processedFiles.
                this.maxProcessedTime = this.processedFiles.size() > 0 ?
                        java.util.Collections.max(this.processedFiles.values()) :
                        this.globalModificationTime;
                // This check is to ensure that maxProcessedTime - readConsistencyOffset > Long.MIN_VALUE
                if (this.maxProcessedTime < Long.MIN_VALUE + this.readConsistencyOffset) {
                    this.maxProcessedTime = Long.MIN_VALUE + this.readConsistencyOffset;
                }
            }

        } else {
            LOG.info("No state to restore for the {}.", getClass().getSimpleName());
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        format.configure(parameters);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Opened {} (taskIdx= {}) for path: {}",
                    getClass().getSimpleName(), getRuntimeContext().getIndexOfThisSubtask(), path);
        }
    }

    @Override
    public void run(SourceFunction.SourceContext<TimestampedFileInputSplit> context) throws Exception {
        Path p = new Path(path);
        FileSystem fileSystem = FileSystem.get(p.toUri());
        if (!fileSystem.exists(p)) {
            throw new FileNotFoundException("The provided file path " + path + " does not exist.");
        }

        checkpointLock = context.getCheckpointLock();
        switch (watchType) {
            case PROCESS_CONTINUOUSLY:
                while (isRunning) {
                    synchronized (checkpointLock) {
                        monitorDirAndForwardSplits(fileSystem, context);
                    }
                    Thread.sleep(interval);
                }

                // here we do not need to set the running to false and the
                // globalModificationTime to Long.MAX_VALUE because to arrive here,
                // either close() or cancel() have already been called, so this
                // is already done.

                break;
            case PROCESS_ONCE:
                synchronized (checkpointLock) {

                    // the following check guarantees that if we restart
                    // after a failure and we managed to have a successful
                    // checkpoint, we will not reprocess the directory.

                    if (globalModificationTime == Long.MIN_VALUE) {
                        monitorDirAndForwardSplits(fileSystem, context);
                        globalModificationTime = Long.MAX_VALUE;
                    }
                    isRunning = false;
                }
                break;
            default:
                isRunning = false;
                throw new RuntimeException("Unknown WatchType" + watchType);
        }
    }

    private void monitorDirAndForwardSplits(FileSystem fs,
                                            SourceContext<TimestampedFileInputSplit> context) throws IOException {
        assert (Thread.holdsLock(checkpointLock));

        Map<Path, FileStatus> eligibleFiles = listEligibleFiles(fs, new Path(path), false);
        this.calculateNextDirScanWindow(eligibleFiles.size());

        Map<Long, List<TimestampedFileInputSplit>> splitsSortedByModTime = getInputSplitsSortedByModTime(eligibleFiles);

        for (Map.Entry<Long, List<TimestampedFileInputSplit>> splits: splitsSortedByModTime.entrySet()) {
            long modificationTime = splits.getKey();
            for (TimestampedFileInputSplit split: splits.getValue()) {
                LOG.info("Forwarding split: " + split);
                context.collect(split);
            }
            // update the global modification time
            maxProcessedTime = Math.max(maxProcessedTime, modificationTime);
        }
        // Populate processed files.
        // This check is to ensure that globalModificationTime will not go backward
        // even if readConsistencyOffset is changed to a large value after a restore from checkpoint,
        // so  files would be processed twice
        globalModificationTime = Math.max(maxProcessedTime - readConsistencyOffset, globalModificationTime);

        processedFiles.entrySet().removeIf(item -> item.getValue() <= globalModificationTime);
        for (FileStatus fileStatus: eligibleFiles.values()) {
            if (fileStatus.getModificationTime() > globalModificationTime) {
                processedFiles.put(fileStatus.getPath().getPath(), fileStatus.getModificationTime());
            }
        }
    }

    /**
     * Creates the input splits to be forwarded to the downstream tasks of the
     * {@link ContinuousFileReaderOperator}. Splits are sorted <b>by modification time</b> before
     * being forwarded and only splits belonging to files in the {@code eligibleFiles}
     * list will be processed.
     * @param eligibleFiles The files to process.
     */
    private Map<Long, List<TimestampedFileInputSplit>> getInputSplitsSortedByModTime(
            Map<Path, FileStatus> eligibleFiles) throws IOException {

        Map<Long, List<TimestampedFileInputSplit>> splitsByModTime = new TreeMap<>();
        if (eligibleFiles.isEmpty()) {
            return splitsByModTime;
        }

        for (FileInputSplit split: format.createInputSplits(readerParallelism)) {
            FileStatus fileStatus = eligibleFiles.get(split.getPath());
            if (fileStatus != null) {
                Long modTime = fileStatus.getModificationTime();
                List<TimestampedFileInputSplit> splitsToForward = splitsByModTime.get(modTime);
                if (splitsToForward == null) {
                    splitsToForward = new ArrayList<>();
                    splitsByModTime.put(modTime, splitsToForward);
                }
                splitsToForward.add(new TimestampedFileInputSplit(
                        modTime, split.getSplitNumber(), split.getPath(),
                        split.getStart(), split.getLength(), split.getHostnames()));
            }
        }
        return splitsByModTime;
    }

    /**
     * Returns the paths of the files not yet processed.
     * @param fileSystem The filesystem where the monitored directory resides.
     */
    private Map<Path, FileStatus> listEligibleFiles(FileSystem fileSystem, Path path, boolean firstScan)
            throws IOException {

        final FileStatus[] statuses;
        try {
            statuses = fileSystem.listStatus(path);
        } catch (IOException e) {
            // we may run into an IOException if files are moved while listing their status
            // delay the check for eligible files in this case
            return Collections.emptyMap();
        }

        if (statuses == null) {
            LOG.warn("Path does not exist: {}", path);
            return Collections.emptyMap();
        } else {
            Map<Path, FileStatus> files = new HashMap<>();
            // handle the new files
            for (FileStatus status : statuses) {
                if (!status.isDir()) {
                    Path filePath = status.getPath();
                    long modificationTime = status.getModificationTime();
                    if (firstScan || !shouldIgnore(filePath, modificationTime)) {
                        files.put(filePath, status);
                    }
                } else if (format.getNestedFileEnumeration() && format.acceptFile(status)){
                    int scanEligibility = this.shouldScanDir(status);
                    if (scanEligibility > 0) {
                        files.putAll(listEligibleFiles(fileSystem, status.getPath(), scanEligibility == 2));
                    }
                }
            }
            return files;
        }
    }

    /**
     * Returns {@code true} if the file is NOT to be processed further.
     * This happens if the modification time of the file is (i) smaller than the {@link #globalModificationTime}
     * or (ii) smaller than {@link #maxProcessedTime} and in the list {@link #processedFiles}
     * @param filePath the path of the file to check.
     * @param modificationTime the modification time of the file.
     */
    private boolean shouldIgnore(Path filePath, long modificationTime) {
        assert (Thread.holdsLock(checkpointLock));
        boolean shouldIgnore =
                (modificationTime <= globalModificationTime) ||
                        ((modificationTime <= maxProcessedTime) &&
                                (processedFiles.containsKey(filePath.getPath()) &&
                                        modificationTime <= processedFiles.get(filePath.getPath())));
        if (shouldIgnore && LOG.isDebugEnabled()) {
            LOG.debug("Ignoring {}, with mod time: {}, watermark: {}, and max mod-time: {}",
                    filePath, modificationTime, globalModificationTime, maxProcessedTime);
        }
        return shouldIgnore;
    }


    public interface DirectoriesPartitioner {
        Tuple2<Long, Long> getDirectoryTimeInfo(FileStatus directory);
        int getForwardRange();
        int getRescanRange();
    }

    /**
     * Check eligibily to scan of a sub directory.
     * The method may be overridden, for e.g. to delay the scan of this sub-directory.
     *
     * @param fileStatus The file status to check.
     * @return
     *      2: first time eligible to scan
     *      1: already scanned, but should scan again
     *      0: should not be scanned
     *      -1: already scanned, to not scan again
     *      -2: have not scanned, but not to scan now
     */
    protected int shouldScanDir(FileStatus fileStatus) {
        if (directoriesPartitioner == null) {
            return 1;
        }
        Tuple2<Long, Long> dirInfo = directoriesPartitioner.getDirectoryTimeInfo(fileStatus);
        Preconditions.checkState(dirInfo.f0 <= dirInfo.f1, "Partition-end must NOT be ahead of parttion-begin");

        // partition is in the future, so set `nearestFutureTimestamp` to the mininum of these,
        // and return -2 so this directory won't be scanned
        if (dirInfo.f0 >= windowEnd) {
            if (nearestFutureTimestamp < windowEnd || dirInfo.f0 < nearestFutureTimestamp) {
                nearestFutureTimestamp = dirInfo.f0;
            }
            return -2;
        }
        // partition is in the past, return -1 so this directory won't be scanned
        if (dirInfo.f1 <= windowBegin) {
            return -1;
        }

        // set `maxCurrentTimestamp`
        if (maxCurrentTimestamp < dirInfo.f0) {
            maxCurrentTimestamp = dirInfo.f0;
        }

        return ((dirInfo.f0 >= prevWindowEnd) ? 2 : 1);
    }

    private long calculateNextDirScanWindow(int previousScanCount) {
        if (directoriesPartitioner == null) {
            return Long.MIN_VALUE;
        }

        prevWindowEnd = windowEnd;
        windowBegin = (maxCurrentTimestamp > Long.MIN_VALUE + directoriesPartitioner.getRescanRange()) ?
                (maxCurrentTimestamp - directoriesPartitioner.getRescanRange()) : Long.MIN_VALUE;
        long temp = Math.max(nearestFutureTimestamp, maxCurrentTimestamp);
        windowEnd = (temp < Long.MAX_VALUE - directoriesPartitioner.getForwardRange()) ?
                (temp + directoriesPartitioner.getForwardRange()) : Long.MAX_VALUE;

        if (LOG.isDebugEnabled()) {
            LOG.debug("(prevWindowEnd, windowBegin, windowEnd): ", prevWindowEnd, windowBegin, windowEnd);
        }
        return prevWindowEnd;
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (checkpointLock != null) {
            synchronized (checkpointLock) {
                globalModificationTime = Long.MAX_VALUE;
                processedFiles.clear();
                isRunning = false;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closed File Monitoring Source for path: " + path + ".");
        }
    }

    @Override
    public void cancel() {
        if (checkpointLock != null) {
            // this is to cover the case where cancel() is called before the run()
            synchronized (checkpointLock) {
                globalModificationTime = Long.MAX_VALUE;
                processedFiles.clear();
                isRunning = false;
            }
        } else {
            globalModificationTime = Long.MAX_VALUE;
            processedFiles.clear();
            isRunning = false;
        }
    }

    //	---------------------			Checkpointing			--------------------------

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(this.globalModificationTime);
        this.checkpointedStateProcessedFilesList.clear();
        this.checkpointedStateProcessedFilesList.add(this.processedFiles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} checkpointed globalModificationTime {}, and {} files.",
                    getClass().getSimpleName(), globalModificationTime, processedFiles.size());
        }
    }
}