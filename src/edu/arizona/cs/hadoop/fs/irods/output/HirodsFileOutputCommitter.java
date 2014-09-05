/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package edu.arizona.cs.hadoop.fs.irods.output;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * An {@link OutputCommitter} that commits files specified in job output
 * directory i.e. ${mapred.output.dir}. 
 *
 */

public class HirodsFileOutputCommitter extends OutputCommitter {

    private static final Log LOG = LogFactory.getLog(HirodsFileOutputCommitter.class);
    
    /**
     * Temporary directory name
     */
    protected static final String TEMP_DIR_NAME = "_temporary";
    private FileSystem workFileSystem = null;
    private FileSystem outputFileSystem = null;
    private Path outputPath = null;
    private Path tempPath = null;
    private Path workPath = null;

    /**
     * Create a file output committer
     *
     * @param outputPath the job's output path
     * @param context the task's context
     * @throws IOException
     */
    public HirodsFileOutputCommitter(Path outputPath, Path tempPath, TaskAttemptContext context) throws IOException {
        if (outputPath != null && tempPath != null) {
            this.outputPath = outputPath;
            this.outputFileSystem = outputPath.getFileSystem(context.getConfiguration());
            this.tempPath = tempPath;
            this.workFileSystem = tempPath.getFileSystem(context.getConfiguration());
            this.workPath = new Path(tempPath,
                    (HirodsFileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR
                    + "_" + context.getTaskAttemptID().toString())).makeQualified(this.workFileSystem);
        }
    }
    
    /**
     * Create the temporary directory that is the root of all of the task work
     * directories.
     *
     * @param context the job's context
     */
    public void setupJob(JobContext context) throws IOException {
        if (this.outputPath != null && this.tempPath != null) {
            Path tmpDir = new Path(this.tempPath, HirodsFileOutputCommitter.TEMP_DIR_NAME);
            FileSystem fileSys = tmpDir.getFileSystem(context.getConfiguration());
            if (!fileSys.mkdirs(tmpDir)) {
                LOG.error("Mkdirs failed to create " + tmpDir.toString());
            }
        }
    }

    /**
     * Delete the temporary directory, including all of the work directories.
     * This is called for all jobs whose final run state is SUCCEEDED
     *
     * @param context the job's context.
     */
    public void commitJob(JobContext context) throws IOException {
        // delete the _temporary folder
        cleanupJob(context);
    }

    @Override
    @Deprecated
    public void cleanupJob(JobContext context) throws IOException {
        if (this.tempPath != null) {
            if (this.workFileSystem.exists(this.tempPath)) {
                this.workFileSystem.delete(this.tempPath, true);
            }
        } else {
            LOG.warn("Temp path is null in cleanup");
        }
    }

    /**
     * Delete the temporary directory, including all of the work directories.
     *
     * @param context the job's context
     * @param state final run state of the job, should be FAILED or KILLED
     */
    @Override
    public void abortJob(JobContext context, JobStatus.State state) throws IOException {
        cleanupJob(context);
    }

    /**
     * No task setup required.
     */
    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
        // temporary task directory is created on demand when the 
        // task is writing.
    }

    /**
     * Move the files from the work directory to the job output directory
     *
     * @param context the task context
     */
    public void commitTask(TaskAttemptContext context) throws IOException {
        TaskAttemptID attemptId = context.getTaskAttemptID();
        if (this.workPath != null) {
            context.progress();
            if (this.workFileSystem.exists(this.workPath)) {
                // Move the task outputs to their final place
                moveTaskOutputsToIRODS(context, this.outputFileSystem, this.outputPath, this.workFileSystem, this.workPath);
                // Delete the temporary task-specific output directory
                if (!this.workFileSystem.delete(this.workPath, true)) {
                    LOG.warn("Failed to delete the temporary output" + " directory of task: " + attemptId + " - " + this.workPath);
                }
                LOG.info("Saved output of task '" + attemptId + "' to " + this.outputPath);
            }
        }
    }

    private void moveTaskOutputsToIRODS(TaskAttemptContext context, FileSystem outfs, Path outDir, FileSystem workfs, Path workOutput) throws IOException {
        context.progress();
        if (workfs.isFile(workOutput)) {
            Path finalOutputPath = getFinalPath(outDir, workOutput, this.workPath);
            FSDataOutputStream irods_os = null;
            FSDataInputStream temp_is = null;
            try {
                // commit to iRODS
                irods_os = outfs.create(finalOutputPath, true);
                temp_is = workfs.open(workOutput);

                byte[] buffer = new byte[100 * 1024];
                int bytes_read = 0;

                while ((bytes_read = temp_is.read(buffer)) != -1) {
                    irods_os.write(buffer, 0, bytes_read);
                }
            } finally {
                if (temp_is != null) {
                    try {
                        temp_is.close();
                    } catch (IOException ex) {
                        // ignore exceptions
                    }
                }

                // remove temporary file
                try {
                    workfs.delete(workOutput, true);
                } catch (IOException ex) {
                    // ignore exceptions
                }

                if (irods_os != null) {
                    irods_os.close();
                }
            }

            LOG.debug("Moved " + workOutput + " to " + finalOutputPath);
        } else if (workfs.getFileStatus(workOutput).isDir()) {
            FileStatus[] paths = workfs.listStatus(workOutput);
            Path finalOutputPath = getFinalPath(outDir, workOutput, this.workPath);
            outfs.mkdirs(finalOutputPath);
            if (paths != null) {
                for (FileStatus path : paths) {
                    moveTaskOutputsToIRODS(context, outfs, outDir, workfs, path.getPath());
                }
            }
        }
    }

    /**
     * Delete the work directory
     */
    @Override
    public void abortTask(TaskAttemptContext context) {
        try {
            if (this.workPath != null) {
                context.progress();
                this.workFileSystem.delete(this.workPath, true);
            }
        } catch (IOException ie) {
            LOG.warn("Error discarding output" + StringUtils.stringifyException(ie));
        }
    }

    /**
     * Find the final name of a given output file, given the job output
     * directory and the work directory.
     *
     * @param jobOutputDir the job's output directory
     * @param taskOutput the specific task output file
     * @param taskOutputPath the job's work directory
     * @return the final path for the specific output file
     * @throws IOException
     */
    private Path getFinalPath(Path jobOutputDir, Path taskOutput, Path taskOutputPath) throws IOException {
        URI taskOutputUri = taskOutput.toUri();
        URI relativePath = taskOutputPath.toUri().relativize(taskOutputUri);
        if (taskOutputUri == relativePath) {
            throw new IOException("Can not get the relative path: base = "
                    + taskOutputPath + " child = " + taskOutput);
        }
        if (relativePath.getPath().length() > 0) {
            return new Path(jobOutputDir, relativePath.getPath());
        } else {
            return jobOutputDir;
        }
    }

    /**
     * Did this task write any files in the work directory?
     *
     * @param context the task's context
     */
    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return this.workPath != null && this.workFileSystem.exists(this.workPath);
    }

    /**
     * Get the directory that the task should write results into
     *
     * @return the work directory
     * @throws IOException
     */
    public Path getWorkPath() throws IOException {
        return this.workPath;
    }
}