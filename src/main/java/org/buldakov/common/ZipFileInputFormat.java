package org.buldakov.common;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class ZipFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    private static boolean IS_LENIENT = false;

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        return new ZipFileRecordReader();
    }

    public static void setLenient(boolean lenient) {
        IS_LENIENT = lenient;
    }

    public static boolean getLenient() {
        return IS_LENIENT;
    }
}
