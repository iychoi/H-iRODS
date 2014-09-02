package edu.arizona.cs.hadoop.fs.irods.output;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import edu.arizona.cs.hadoop.fs.irods.util.HirodsConfigUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class HirodsFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

    private HirodsFileOutputCommitter committer = null;

    public static Path getWorkOutputPath(TaskInputOutputContext<?, ?, ?, ?> context) throws IOException, InterruptedException {
        HirodsFileOutputCommitter committer = (HirodsFileOutputCommitter) context.getOutputCommitter();
        return committer.getWorkPath();
    }

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        HirodsFileOutputCommitter committer = (HirodsFileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getUniqueFile(context, getOutputName(context), extension));
    }
    
    public static void setTempPath(Job job, Path outputDir) {
        HirodsConfigUtils.setIrodsOutputBufferedPath(job.getConfiguration(), outputDir.toString());
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (this.committer == null) {
            Path output = getOutputPath(context);
            String tempPath = HirodsConfigUtils.getIrodsOutputBufferedPath(context.getConfiguration());
            Path temp = new Path(tempPath);
            this.committer = new HirodsFileOutputCommitter(output, temp, context);
        }
        return this.committer;
    }
}

