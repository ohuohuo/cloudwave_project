package reduce1;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileInputFormat;  
import org.apache.hadoop.mapred.FileOutputFormat;  
import org.apache.hadoop.mapred.JobClient;  
import org.apache.hadoop.mapred.JobConf;    
import org.apache.hadoop.mapred.TextInputFormat;  
import org.apache.hadoop.mapred.TextOutputFormat; 
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MAPREDUCE_JSON {
	static double num_segments;
	static Connection conn = null;
	
public static void getSegment(String Reduce_key, Connection conn){
		
		String Channel_id = "'"+Reduce_key+"'";
		System.out.println("channel_id:"+Channel_id);
		//String Channel_id = "'FT10'";

		int study_id = 0;
		int nDR = 0;
		float dDR = 0;

		try {
			if(conn != null) {
				System.out.println("Connection not null inside map");
				Statement statement = conn.createStatement();
			
				ResultSet rs_channel = statement.executeQuery("SELECT * FROM channels where label ="+Channel_id);
			
				while(rs_channel.next())
						study_id = rs_channel.getInt("study");
			
				ResultSet rs_STUDY = statement.executeQuery("SELECT * FROM STUDIES where study_id ="+study_id);
			
				while(rs_STUDY.next()){
						nDR = rs_STUDY.getInt("num_datarecords");
						dDR = rs_STUDY.getFloat("duration_datarecord");
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		num_segments = Math.ceil((dDR*nDR)/30);
		//return num_segments;
	}

	public static class JSONMap extends MapReduceBase implements Mapper<Text, BytesWritable, TextInt, Text>{
		  public static int[] byteArrayToInt(byte[] bytes) {  //This function is to convert byte array into an int array
		        // for (int b=0; b<barr.length; b++) { System.out.printf("%X ",barr[b]); }
			  	
		        // Pad the size to multiple of 2
		        int size = (bytes.length / 2) + ((bytes.length % 2 == 0) ? 0 : 1);
		        
		        ByteBuffer bb = ByteBuffer.allocate(size * 2);
		        bb.put(bytes);
		        
		        // Set Byte Order to Little Endian
		        bb.order(ByteOrder.LITTLE_ENDIAN);
		        
		        int[] result = new int[size];
		        bb.rewind();
		        while (bb.remaining() > 0) {
		            result[bb.position() / 2] = (int) bb.getShort();
		        }
		        // for (int b=0; b<result.length; b++) { System.out.printf("%d ",result[b]); }
		        return result;
		    }
		  
		  public static JSONArray jsonArrayTest(int[] intarray) {       // This function is to convert int array to json file
		        JSONArray jsonarray = new JSONArray();
		        try{
		            for (int i = 0; i < intarray.length; i++) {
		                jsonarray.put(intarray[i]); }}catch(Exception e){
		                    e.printStackTrace();
		                }
		            //print this json out
		        return jsonarray;
		    }
		  //generate time from raw data
		  protected static String readTime(int time){
		    	int sec = time * 30;
	            int min = 0;
	            int hour = 0;
	            
	            if(sec >= 60 && sec < 3600)
	            {
	                min = (int)Math.floor(sec/60);
	                sec = sec % 60;
	            } else if(sec >= 3600){
	                hour = (int)Math.floor(sec/3600);
	                sec = sec % 3600;
	                min = (int)Math.floor(sec/60);
	                sec = sec % 60;
	            }
	            DecimalFormat secFor = new DecimalFormat("00");
	            String secStr = new String(secFor.format(sec));
	            DecimalFormat minFor = new DecimalFormat("00");
	            String minStr = new String(minFor.format(min));
	            DecimalFormat hourFor = new DecimalFormat("00");
	            String hourStr = new String(hourFor.format(hour));
	            String timeStr = new String(hourStr+":"+minStr+":"+secStr);
	            return timeStr;
		    }
		
		@Override
		public void map(Text key, BytesWritable value, OutputCollector<TextInt, Text> output,
				Reporter reporter) throws IOException {
			
			System.out.println(key.toString());
			//below is the code to output the intermediate data
            		getSegment(key.toString(),conn);
			System.out.println(key.toString());
			// add content
        		//how many bytes are there in the value
        	System.out.println("map invoked");

        	System.out.println(num_segments);

        	TextInt tx=new TextInt();
			Text Textvalue = new Text();
        	
        	int len = value.getLength();
        	int size = (len / 2) + ((len % 2 == 0) ? 0 : 1);
            int[] intArray = new int[size];
            int lengthOfSegment;//length of each segment
            int lengthOfRemain;//length of remainings 
           
            intArray = byteArrayToInt(value.copyBytes());
            
            lengthOfSegment = (int)Math.floor(size/num_segments);
            lengthOfRemain = (int)(size - lengthOfSegment*num_segments);
            //String data = new String("data");
            //String segment_start_time = new String("segment_start_time");
            for(int i = 0; i < num_segments - 1; i++){
            	int[] temp = new int[lengthOfSegment];
            	System.arraycopy(intArray, i*lengthOfSegment, temp, 0, lengthOfSegment);
            	//System.out.println(temp);
            	try {
            		JSONObject jsonDataPoint = new JSONObject();
					
            		jsonDataPoint.put("segment", i);
					jsonDataPoint.append("segment_start_time", readTime(i));
					jsonDataPoint.append("data", jsonArrayTest(temp));
					//String mulKey = key.toString()+"/"+i;
					tx.setFirstKey(key.toString());
					tx.setSecondKey(i);
					//System.out.println(key.toString()+i+","+jsonDataPoint.toString());
					//below is the code to output the intermediate data
					
					output.collect(tx, new Text(jsonDataPoint.toString()));
            	    
					//System.out.println(mulKey+jsonDataPoint.toString());
					
            	} catch (JSONException e) {
					e.printStackTrace();
				}
            }
            
            if(lengthOfRemain > 0){
            	int[] temp = new int[lengthOfRemain];
            	System.arraycopy(intArray, (int) (lengthOfSegment*(num_segments -1)), temp, 0, lengthOfRemain);
            	try {
            		JSONObject jsonDataPoint = new JSONObject();
            		
            		jsonDataPoint.put("segment", (int)num_segments);
            		jsonDataPoint.append("segment_start_time", readTime((int)num_segments));
            		jsonDataPoint.append("data", jsonArrayTest(temp));
					
					//String mulKey = key.toString()+"/"+i;
					tx.setFirstKey(key.toString());
					tx.setSecondKey((int)num_segments);
					
					//below is the code to output the intermediate data
					/*
					out.write(key.toString()+"-"+(int)num_segments+","+jsonDataPoint.toString());
		            out.flush();   
		            */
					
					output.collect(tx, new Text(jsonDataPoint.toString()));
            	    
					//System.out.println(mulKey+jsonDataPoint.toString());
					
            	} catch (JSONException e) {
					e.printStackTrace();
				}
            }
        }
	
	}
	
	public static class JSONReduce extends MapReduceBase implements Reducer<TextInt,Text,Text,Text>{
		
		@Override
		public void reduce(TextInt key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			JSONObject Final_JSON = new JSONObject();
			JSONObject Segment_num = new JSONObject();
			//Final_JSON = DBHEADER(key.firstKey);
			Final_JSON = DBHEADER(key.firstKey,conn);
					
			Text Textkey = new Text();
			Text Textvalue = new Text();

			JSONObject signal= new JSONObject();
			try {
				signal = Final_JSON.getJSONObject("signal");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JSONObject datasegments= new JSONObject();
			try {
				datasegments = signal.getJSONObject("datasegments");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			JSONArray data_points= new JSONArray();
			try {
				data_points = datasegments.getJSONArray("data_points");
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			while (values.hasNext()){
				data_points.put(values.next().toString());
			}
			Textvalue.set(Final_JSON.toString());
			Textkey.set("");
			output.collect(Textkey, Textvalue);
		}
		
	}

	public static class TextInt implements WritableComparable{

		private String firstKey;
		private int secondKey;

		public String getFirstKey(){
			return firstKey;
		}
		public void setFirstKey(String firstKey){
			this.firstKey = firstKey;
		}
		
		public int getSecondKey() {
	        return secondKey;
	    }
	    public void setSecondKey(int secondKey) {
	        this.secondKey = secondKey;
	    }
	    
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			firstKey=in.readUTF();
	        secondKey=in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(firstKey);
	        out.writeInt(secondKey);
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			TextInt ti=(TextInt)o;
			return this.getFirstKey().compareTo(ti.getFirstKey());
		}	
	}
	
	//map sort//
	public static class TextComparator extends WritableComparator{
		public TextComparator() {
			super(TextInt.class,true);	
		}
		 public int compare(WritableComparable a, WritableComparable b) {
		        // TODO Auto-generated method stub
		        TextInt ti1=(TextInt)a;
		        TextInt ti2=(TextInt)b;
		        return ti1.getFirstKey().compareTo(ti2.getFirstKey());
		    }
	}
	
	//reduce sort//
	public static class TextIntComparator extends WritableComparator {
		public TextIntComparator(){
	        super(TextInt.class,true);
	    }
	    @Override
	    public int compare(WritableComparable a, WritableComparable b) {
	        TextInt ti1=(TextInt)b;
	        TextInt ti2=(TextInt)a;

	        //gurantte that they must be in one group, the flag is that the 1st field should be the same
	        if(!ti1.getFirstKey().equals(ti2.getFirstKey()))
	           return ti1.getFirstKey().compareTo(ti2.getFirstKey());
	        else
	           return ti2.getSecondKey() - ti1.getSecondKey();//0,-1,1
	    }
	                                                                                                                                                          
	}
	
	public static class KeyPartitioner implements Partitioner<TextInt, Text> {
		
		@Override
		public void configure(JobConf conf) {
			// TODO Auto-generated method stub
			
		}
		@Override
		public int getPartition(TextInt key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return (key.getFirstKey().hashCode()&Integer.MAX_VALUE)%numPartitions;
		}
	}
	
	public static void DBCONNECT(){
		String driver = "com.mysql.jdbc.Driver";
        String url ="jdbc:mysql://hpcc-login.case.edu/cloudtest";
        String user = "eecs600";
        String password = "ha@oopR0ll";
        
        try{
            Class.forName(driver);
            conn = DriverManager.getConnection(url,user.trim(),password.trim());
            if(!conn.isClosed())
                    System.out.println("Succeeded connecting to the Database!");
        }
        catch (Exception e){
            System.out.print("get data error!");
            e.printStackTrace();
    }
	}

	public static JSONObject DBHEADER(String Reduce_key, Connection conn){

		String datarecords = "";
		
		//int Channel_id = Integer.parseInt(id[2]);
		String Channel_id = "'"+Reduce_key+"'";
		//String Channel_id = "'FT10'";

		int study_id = 0;
		int patient_id = 0;
		int nDR = 0;
		float dDR = 0;
		
		JSONObject FINAL_Json = new JSONObject();
		
        /*try{
                Class.forName(driver);
                Connection conn = DriverManager.getConnection(url,user.trim(),password.trim());
                if(!conn.isClosed())
                        System.out.println("Succeeded connecting to the Database!");*/
		try{

                Statement statement = conn.createStatement();
                ResultSet rs_channel = statement.executeQuery("SELECT * FROM channels where label ="+Channel_id);
                JSONObject Channel_json = new JSONObject();
                
                
                while(rs_channel.next()){
                Channel_json.put("id", rs_channel.getInt("channel_id"));
                Channel_json.put("label", rs_channel.getString("label"));
               	Channel_json.put("transducer_type", rs_channel.getString("transducer_type"));
               	Channel_json.put("physical_dimension", rs_channel.getString("physical_dimension"));
              	Channel_json.put("physical_minimum", rs_channel.getString("physical_minimum"));
               	Channel_json.put("physical_maximum", rs_channel.getString("physical_maximum"));
               	Channel_json.put("digital_minimum", rs_channel.getString("digital_minimum"));
               	Channel_json.put("digital_maximum", rs_channel.getString("digital_maximum"));
               	Channel_json.put("num_samples", rs_channel.getString("num_samples"));
               	Channel_json.put("reserved", rs_channel.getString("reserved"));
                
               	study_id = rs_channel.getInt("study");             
                }

                ResultSet rs_STUDY = statement.executeQuery("SELECT * FROM STUDIES where study_id ="+study_id);
                JSONObject STUDY_json = new JSONObject();
                while(rs_STUDY.next()){
                	STUDY_json.put("id", rs_STUDY.getString("name"));
                	STUDY_json.put("version", rs_STUDY.getString("version"));
                	STUDY_json.put("local_patient_id", rs_STUDY.getString("local_patientID"));
                	STUDY_json.put("start_datetime_of_recording", rs_STUDY.getString("start_datetime_of_recording"));
                	STUDY_json.put("num_headerbytes", rs_STUDY.getString("num_headerbytes"));
                	STUDY_json.put("duration_of_datarecord", rs_STUDY.getString("duration_datarecord"));
                	STUDY_json.put("unit_datarecord_duration", "sec");
                	STUDY_json.put("reserved", rs_STUDY.getString("reserved"));
                	STUDY_json.put("num_datarecords", rs_STUDY.getString("num_datarecords"));
                	STUDY_json.put("num_signals", rs_STUDY.getString("num_signals"));
                	STUDY_json.put("num_epochs", rs_STUDY.getString("num_epochs"));
                	
                	 FINAL_Json.put("study", STUDY_json);
                	 patient_id = rs_STUDY.getInt("patient");
                	 nDR = rs_STUDY.getInt("num_datarecords");
                	 dDR = rs_STUDY.getFloat("duration_datarecord");  
                }
                
                num_segments = Math.ceil((dDR*nDR)/30);
                
                ResultSet rs_PATIENTS = statement.executeQuery("SELECT * FROM PATIENTS where patient_id ="+patient_id);
                JSONObject PATIENTS_json = new JSONObject();
                while(rs_PATIENTS.next()){
                	PATIENTS_json.put("id", rs_PATIENTS.getString("name"));
                	FINAL_Json.put("patient", PATIENTS_json);
                }
                
                JSONArray DATARECORDS = new JSONArray();
               /*for(int i = 1; i < nDR+1; i++){
               // for(int i = 1; i < 5; i++){
                	 DATARECORDS.put(i);
                }*/
                Channel_json.put("datarecords", DATARECORDS);
                
                JSONObject datasegments = new JSONObject();
                datasegments.put("duration_of_segment","30");
                datasegments.put("unit_segment_duration","sec");
                datasegments.put("num_segments",num_segments);
                JSONArray data_points = new JSONArray();
                JSONArray data = new JSONArray();
               
             /* for(int i = 1; i<num_segments+1; i++){
                	JSONObject one_data_points = new JSONObject();
                     
                	one_data_points.put("segment", i);
                	one_data_points.put("segment_start_time", "HH:MM:SS");
                	for (int j = 1 ; j<6001 ;j++){
                		data.put((i-1)*6000 + j);
                	}
                	one_data_points.put("data",data);
                	data_points.put(one_data_points);
                }*/
                
                datasegments.put("data_points",data_points);
            	Channel_json.put("datasegments", datasegments);
                FINAL_Json.put("signal", Channel_json); 
               
                System.out.println(FINAL_Json.toString());
                
                /*while(rs.next()){
                        System.out.println(rs.getInt("patient_id"));
                        System.out.println(rs.getString("name"));
                }*/
		}
        catch (Exception e){
                System.out.print("get data error!");
                e.printStackTrace();
        }
        return FINAL_Json;
	}
		
	public static void main (String[] args) throws Exception{
		/*int exitCode = ToolRunner.run( new MAPREDUCE_JSON( ), args);
		System.exit(exitCode);*/
		
		JobConf conf = new JobConf(MAPREDUCE_JSON.class);
		conf.setJobName("Convert JSON");
		FileSystem fs= FileSystem.get(conf);
		DBCONNECT();
		
		//String[] arg = new String[2];
		 
		//arg[0] = "JSON/input";
	    //arg[1] = "JSON/output";
		 
		//output = arg[1];
		
		conf.setMapOutputKeyClass(TextInt.class);
		conf.setMapOutputValueClass(Text.class);
		
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(Text.class);
	    
	    conf.setPartitionerClass(KeyPartitioner.class);
	    conf.setOutputValueGroupingComparator(TextComparator.class);
	    conf.setOutputKeyComparatorClass(TextIntComparator.class);
	    
	    conf.setMapperClass(JSONMap.class);
	    conf.setReducerClass(JSONReduce.class);

	    conf.setInputFormat(JSONFileInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    
	    
	    //FileInputFormat.addInputPath(conf, new Path("JSON/input"));
		//FileOutputFormat.setOutputPath(conf, new Path("JSON/output"));
	    Path inputPath = new Path(args[0]);
	    FileStatus[] status_list = fs.listStatus(inputPath);
        if(status_list != null){
            for(FileStatus status : status_list){
                //add each file to the list of inputs for the map-reduce job
               FileInputFormat.addInputPath(conf, status.getPath());
            }
        }
	    //FileInputFormat.addInputPath(conf, new Path(args[0]));
	  	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	    
	    JobClient.runJob(conf);

	}
}
