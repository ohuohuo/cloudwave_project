package reduce1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JSONRecordReader implements RecordReader<Text, BytesWritable> {
	private FileSplit fileSplit;
    private Configuration conf;
    
    private final Text currKey = new Text();
    private final BytesWritable currValue = new BytesWritable();
    private boolean fileProcessed = false;
    
    public JSONRecordReader() throws IOException {
    }
    public JSONRecordReader(FileSplit split, Configuration conf) throws IOException {
    	this.fileSplit = split;
    	this.conf = conf;
    }
    
    public JSONRecordReader(FileSplit split, JobConf conf) throws IOException {
    	this.fileSplit = split;
    	this.conf = conf;
    }
    
    
    public boolean next(Text Key, BytesWritable value) throws IOException {
        if ( fileProcessed ){
            return false;
        }
        
        int fileLength = (int)fileSplit.getLength();
        byte [] result = new byte[fileLength];
        //static int num_split = 0;
        
        
        FileSystem  fs = FileSystem.get(conf);
        FSDataInputStream in = null;
        try {
        	
        	System.out.println("nextKeyValue invoked");
        	
        	
            in = fs.open( fileSplit.getPath());
			IOUtils.readFully(in, result, 0, fileLength);
            currKey.set(fileSplit.getPath().getName());
            currValue.set(result, 0, fileLength);
        } finally {
            IOUtils.closeStream(in);
        }
        this.fileProcessed = true;
        //num_split++;
        return true;
    }
    public Text createKey() {
        //return NullWritable.get();
        return currKey;
    }
    public BytesWritable createValue() {
        return currValue;
    }
    public float getProgress() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }
    public void close() throws IOException {
        // nothing to close
    }
    public long getPos() throws IOException{
    	return fileProcessed ? fileSplit.getLength(): 0;
    }


	/*public void initialize(InputSplit inputSplit, JobConf conf)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		 this.split = (FileSplit)inputSplit;
	        this.conf = conf.getConfiguration();
	}*/
	
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.fileSplit = (FileSplit)inputSplit;
        this.conf = context.getConfiguration();
	}
}
