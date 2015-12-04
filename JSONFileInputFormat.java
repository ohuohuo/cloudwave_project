package reduce1;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class JSONFileInputFormat extends FileInputFormat<Text, BytesWritable>{
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
}

/*public RecordReader<Text, BytesWritable> createRecordReader( 
		InputSplit inputSplit, TaskAttemptContext context) throws IOException,
        InterruptedException {
    JSONRecordReader reader = new JSONRecordReader();
    reader.initialize(inputSplit, context);
    return reader;
}*/
		

public RecordReader<Text, BytesWritable> getRecordReader(
InputSplit split, JobConf job, Reporter reporter)
throws IOException {
return new JSONRecordReader((FileSplit) split, job);
}
}