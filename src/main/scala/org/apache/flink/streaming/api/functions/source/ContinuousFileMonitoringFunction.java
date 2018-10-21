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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDate;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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
    private final String basePath;

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

    /** The current directory watermark, used to skip reading old directories */

    public static final Instant IGNORE_THIS_DIRECTORY = Instant.ofEpochMilli(Long.MIN_VALUE);;
    public static final Instant MIN_DIR_TIMESTAMP = Instant.ofEpochMilli(Long.MIN_VALUE + 1000);;
    private volatile Instant dirWatermark = MIN_DIR_TIMESTAMP;

    /** Max amount of time between latest directory and the directory that will be eligible to scan */
    private final long dirScanChunkSizeInSeconds;
    private final long dirRescanIntervalInSeconds;

    /** Max amount of time between latest directory and the directory that will be eligible to scan */

    /** The list of processed files having modification time within the period from globalModificationTime
     *  to maxProcessedTime in the form of a Map&lt;filePath, lastModificationTime&gt;. */
    private volatile Map<String, Long> processedFiles;

    private transient Object checkpointLock;

    private volatile boolean isRunning = true;

    private transient ListState<Long> checkpointedState;

    private transient ListState<Long> checkpointedStateDirWatermark;

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
        this(format, watchType, readerParallelism, interval, readConsistencyOffset, Long.MAX_VALUE);
    }

    public ContinuousFileMonitoringFunction(
            FileInputFormat<OUT> format,
            FileProcessingMode watchType,
            int readerParallelism,
            long interval,
            long readConsistencyOffset,
            long directoryRescanInterval) {

        LOG.warn("HUYEN Monitoring {}: {} ", format.getFilePaths().length, format.getFilePaths()[0].toString());

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
        this.basePath = new Path(path).getPath() + "/";

        this.interval = interval;
        this.watchType = watchType;
        this.readerParallelism = Math.max(readerParallelism, 1);
        this.readConsistencyOffset = readConsistencyOffset;
        this.globalModificationTime = Long.MIN_VALUE;
        this.maxProcessedTime = Long.MIN_VALUE + this.readConsistencyOffset;
        this.processedFiles = new HashMap<>();

        if (this.readConsistencyOffset > 10) {
            this.dirScanChunkSizeInSeconds = 900L;
            this.dirRescanIntervalInSeconds = 8 * dirScanChunkSizeInSeconds;
        } else {
            this.dirScanChunkSizeInSeconds = 0L;
            this.dirRescanIntervalInSeconds = Integer.MAX_VALUE;
        }
        dirWatermark = dirWatermark.plusSeconds(dirRescanIntervalInSeconds);
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
        this.checkpointedStateDirWatermark = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "directory-watermark",
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
            List<Long> retrievedStates3 = new ArrayList<>();
            for (Long entry : this.checkpointedStateDirWatermark.get()) {
                retrievedStates3.add(entry);
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

                if (retrievedStates3.size() == 1) {
                    this.dirWatermark = Instant.ofEpochMilli(retrievedStates3.get(0));
                    LOG.info("{} retrieved a directory watermark for {} of {}.",
                            getClass().getSimpleName(), this.path, dirWatermark);
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

        Map<Path, FileStatus> eligibleFiles;
        if (dirScanChunkSizeInSeconds > 0) {
            eligibleFiles = new HashMap<>();
            Map<Instant, List<FileStatus>> availableDirs = listDirsAfterTimestamp(fs, new Path(path),
                    dirWatermark.minusSeconds(dirRescanIntervalInSeconds));

            if (availableDirs.size() > 0) {
                Instant lastestTimestamp = dirWatermark.minusSeconds(dirRescanIntervalInSeconds);
                Instant curWindowEnd = lastestTimestamp.plusSeconds(dirScanChunkSizeInSeconds);

                for (Map.Entry<Instant, List<FileStatus>> directories : availableDirs.entrySet()) {
                    Instant curDirTimestamp = directories.getKey();
                    if (!curWindowEnd.isAfter(curDirTimestamp)) {
                        if (eligibleFiles.size() > 0) {
                            break;
                        } else {
                            curWindowEnd = curDirTimestamp.plusSeconds(dirScanChunkSizeInSeconds);
                        }
                    }
                    lastestTimestamp = curDirTimestamp;
                    for (FileStatus directory : directories.getValue()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Listing files in dir: " + directory);
                        }
                        eligibleFiles.putAll(listEligibleFiles(fs, directory.getPath()));
                    }
                }

                if (eligibleFiles.size() > 0 && lastestTimestamp.isAfter(dirWatermark)) {
                    LOG.warn("{} has dir watermark being advanced from {} to {}", path, dirWatermark, lastestTimestamp);
                    dirWatermark = lastestTimestamp;
                }
            }

        } else {
            eligibleFiles = listEligibleFiles(fs, new Path(path));
        }

        Map<Long, List<TimestampedFileInputSplit>> splitsSortedByModTime = getInputSplitsSortedByModTime(eligibleFiles);

        for (Map.Entry<Long, List<TimestampedFileInputSplit>> splits: splitsSortedByModTime.entrySet()) {
            long modificationTime = splits.getKey();
            for (TimestampedFileInputSplit split: splits.getValue()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Forwarding split: " + split);
                }
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

        if (eligibleFiles.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Long, List<TimestampedFileInputSplit>> splitsByModTime = new TreeMap<>();

        // returns if unsplittable
        if (format.getNumSplits() == 1) {
            int splitNum = 0;
            for (final FileStatus file : eligibleFiles.values()) {
                if (format.acceptFile(file)) {
                    final FileSystem fs = file.getPath().getFileSystem();
                    final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
                    Set<String> hosts = new HashSet<String>();
                    for (BlockLocation block : blocks) {
                        hosts.addAll(Arrays.asList(block.getHosts()));
                    }

                    Long modTime = file.getModificationTime();
                    splitsByModTime.computeIfAbsent(modTime, k -> new ArrayList<>())
                            .add(new TimestampedFileInputSplit(modTime, splitNum++, file.getPath(),
                                    0, -1, hosts.toArray(new String[hosts.size()])));
                }
            }
        } else {
            for (FileInputSplit split : format.createInputSplits(readerParallelism)) {
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
        }
        return splitsByModTime;
    }

    /**
     * Returns the paths of the files not yet processed.
     * @param fileSystem The filesystem where the monitored directory resides.
     */
    private Map<Path, FileStatus> listEligibleFiles(FileSystem fileSystem, Path path) throws IOException {

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
                if (format.acceptFile(status)) {
                    if (!status.isDir()) {
                        Path filePath = status.getPath();
                        long modificationTime = status.getModificationTime();
                        if (!shouldIgnore(filePath, modificationTime)) {
                            files.put(filePath, status);
                        }
                    } else if (format.getNestedFileEnumeration()) {
                        files.putAll(listEligibleFiles(fileSystem, status.getPath()));
                    }
                }
            }
            return files;
        }
    }
    /**
     * Returns the paths of the files not yet processed.
     * @param fileSystem The filesystem where the monitored directory resides.
     */
    private Map<Instant, List<FileStatus>> listDirsAfterTimestamp(FileSystem fileSystem, Path path, Instant watermark) throws IOException {

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
        } else if (statuses.length == 0) {
            return Collections.emptyMap();
        } else {
            Map<Instant, List<FileStatus>> dirsSortedByTimestamp = new TreeMap<>();
            // handle the new files
            for (FileStatus status : statuses) {
                if (status.isDir() && format.acceptFile(status)) {
                    ChronoUnit curDirLevel = getDirectoryLevel(status);
                    Instant curDirTimestamp = getDirectoryTimestamp(status);
                    switch (curDirLevel) {
                        case FOREVER:
                            listDirsAfterTimestamp(fileSystem, status.getPath(), watermark).forEach((k, v) ->
                                    dirsSortedByTimestamp.merge(k, v, (v1, v2) -> {
                                        v1.addAll(v2);
                                        return v1;
                                    }));
                            break;
                        case SECONDS:
                        case MILLIS:
                        case MICROS:
                        case NANOS:
                            if (!watermark.isAfter(curDirTimestamp)) {
                                List<FileStatus> tmpDirsList = dirsSortedByTimestamp.get(curDirTimestamp);
                                if (tmpDirsList == null) {
                                    tmpDirsList = new ArrayList<>();
                                    dirsSortedByTimestamp.put(curDirTimestamp, tmpDirsList);
                                }
                                tmpDirsList.add(status);
                            }
                            break;
                        default:
                            if (!watermark.truncatedTo(curDirLevel).isAfter(curDirTimestamp)) {
                                listDirsAfterTimestamp(fileSystem, status.getPath(), watermark).forEach((k, v) ->
                                        dirsSortedByTimestamp.merge(k, v, (v1, v2) -> {
                                            v1.addAll(v2);
                                            return v1;
                                        }));
                            }
                    }
                }
            }
            return dirsSortedByTimestamp;
        }
    }

    protected Instant getDirectoryTimestamp(FileStatus directory) {
        String relativePath = directory.getPath().getPath().split(basePath, 2)[1];
        String dirs[] = relativePath.split("/");
        try {
            if (dirs.length == 1) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
                return LocalDate.parse(relativePath, formatter).atStartOfDay().toInstant(ZoneOffset.UTC);
            } else {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd/HHmm").withZone(ZoneOffset.UTC);
                return ZonedDateTime.parse(relativePath, formatter).toInstant();
            }
        } catch (java.time.format.DateTimeParseException ex) {
            return IGNORE_THIS_DIRECTORY;
        }
    }

    protected ChronoUnit getDirectoryLevel(FileStatus directory) {
        String relativePath = directory.getPath().getPath().split(basePath, 2)[1];
        String dirs[] = relativePath.split("/");
        switch (dirs.length) {
            case 1: return ChronoUnit.DAYS;
            case 2: return ChronoUnit.SECONDS;
            default: return ChronoUnit.FOREVER;
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
        this.checkpointedStateDirWatermark.clear();
        this.checkpointedStateDirWatermark.add(this.dirWatermark.toEpochMilli());

        if (LOG.isDebugEnabled()) {
            LOG.debug("{} checkpointed globalModificationTime {}, and {} files.",
                    getClass().getSimpleName(), globalModificationTime, processedFiles.size());
        }
    }
}
