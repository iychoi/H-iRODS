package org.apache.hadoop.fs.irods.output;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.irods.HirodsFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.StringUtils;

public class HirodsFileOutputCommitter extends FileOutputCommitter {

    private static final Log LOG = LogFactory.getLog(HirodsFileOutputCommitter.class);

    private boolean useIRODS = false;
    private Path workPath = null;
    private Path outputPath = null;
    private FileSystem workFileSystem = null;
    private FileSystem outputFileSystem = null;

    public HirodsFileOutputCommitter(Path outputPath, Path tempPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
        this.workPath = tempPath;
        this.outputPath = outputPath;
        
        if (outputPath != null) {
            FileSystem ofs = outputPath.getFileSystem(context.getConfiguration());
            if (ofs instanceof HirodsFileSystem) {
                this.useIRODS = true;
                this.outputFileSystem = ofs;
                this.workFileSystem = tempPath.getFileSystem(context.getConfiguration());
                this.workPath = new Path(tempPath,
                        (FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR
                        + "_" + context.getTaskAttemptID().toString())).makeQualified(this.workFileSystem);
            } else {
                this.useIRODS = false;
            }
        }
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        if(this.useIRODS) {
            Path tmpDir = new Path(this.workPath, FileOutputCommitter.TEMP_DIR_NAME);
            FileSystem fileSys = tmpDir.getFileSystem(context.getConfiguration());
            if (!fileSys.mkdirs(tmpDir)) {
                LOG.error("Mkdirs failed to create " + tmpDir.toString());
            }
        } else {
            super.setupJob(context);
        }
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException {
        if(this.useIRODS) {
            Path tmpDir = new Path(this.workPath, FileOutputCommitter.TEMP_DIR_NAME);
            FileSystem fileSys = tmpDir.getFileSystem(context.getConfiguration());
            if (fileSys.exists(tmpDir)) {
                fileSys.delete(tmpDir, true);
            }
        } else {
            super.cleanupJob(context);
        }
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        if(this.useIRODS) {
            TaskAttemptID attemptId = context.getTaskAttemptID();
            context.progress();
            if (this.workFileSystem.exists(this.workPath)) {
                // Move the task outputs to their final place
                moveTaskOutputsToIRODS(context, this.outputFileSystem, this.outputPath, this.workFileSystem, this.workPath);
                // Delete the temporary task-specific output directory
                if (!this.workFileSystem.delete(this.workPath, true)) {
                    LOG.warn("Failed to delete the temporary output"
                            + " directory of task: " + attemptId + " - " + this.workPath);
                }
                LOG.info("Saved output of task '" + attemptId + "' to "
                        + this.outputPath);
            }
        } else {
            super.commitTask(context);
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

    @Override
    public void abortTask(TaskAttemptContext context) {
        if(this.useIRODS) {
            try {
                if (this.workPath != null) {
                    context.progress();
                    this.workFileSystem.delete(this.workPath, true);
                }
            } catch (IOException ie) {
                LOG.warn("Error discarding output" + StringUtils.stringifyException(ie));
            }
        } else {
            super.abortTask(context);
        }
    }

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

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        if(this.useIRODS) {
            return this.workPath != null && this.workFileSystem.exists(this.workPath);
        } else {
            return super.needsTaskCommit(context);
        }
    }

    @Override
    public Path getWorkPath() throws IOException {
        return this.workPath;
    }
}
