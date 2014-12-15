package com.binbo.hcompute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

public class HCompute {
	
	// Logger
	private static Logger log = Logger.getLogger(HCompute.class);
	
	// Index to name
	final static String[] AIR_NAMES = {
		"9E", "AA", "AQ", "AS", "B6", "CO", "DL", "EV", "F9", "FL", 
		"HA", "MQ", "NW", "OH", "OO", "UA", "US", "WN", "XE", "YV"
	};
	
	// HBase table name, column family and qualifiers
	private final static String TABLE_NAME = "Flights";
	private final static byte[] COLUMN_FAMILY   = new byte[] { (byte) 'd' };
	private final static byte[] COND_QUALIFIER  = new byte[] { (byte) 'c' };
	private final static byte[] OTHER_QUALIFIER = new byte[] { (byte) 'o' };
	
	public static class HComputeKey implements WritableComparable<HComputeKey> {
		
		// The flight airline id
		public String mAirName;
		
		// The month for the flight (for secondary sort)
		public int mMonth;		
		
	    public HComputeKey() {
	    }

	    public void set(String airName, int month) {	    	
	    	mMonth   = month;
	    	mAirName = airName;
	    }

	    ///////////////////////////////////////////// Implement Writable
	    public void write(DataOutput out) throws IOException {
	    	out.writeInt(mMonth);
	    	out.writeUTF(mAirName);
	    }

	    ///////////////////////////////////////////// Implement Writable
	    public void readFields(DataInput in) throws IOException {
	    	mMonth   = in.readInt();
	    	mAirName = in.readUTF();	    	
	    }

	    ///////////////////////////////////////////// Implement Comparable
	    public int compareTo(HComputeKey obj) {   
	    	int result = mAirName.compareTo(obj.mAirName);
	    	if (result == 0) {
	        	result = mMonth - obj.mMonth;
	        }
	        return result;
	    }
	}
	
	public static class HComputeValue implements Writable {		  					
		
		// The month for the flight
		public int mMonth;
		
		// The flight count
		public int mCount;
		
		// The actual monthly delay in minute
		public double mDelay;
		
		public HComputeValue() {
		}
		
		public void set(int month, int count, double delay) {
			mMonth = month;
			mCount = count;
			mDelay = delay;
		}

		public void write(DataOutput out) throws IOException {
		    out.writeInt(mMonth);
		    out.writeInt(mCount);
		    out.writeDouble(mDelay);
		}

		public void readFields(DataInput in) throws IOException {
			mMonth = in.readInt();
			mCount = in.readInt();
			mDelay = in.readDouble();
		}	
	}
		
	// The partitioner uses airline name to split the data to the reducer
	public static class HComputePartitioner extends Partitioner<HComputeKey, HComputeValue> {				
		
	    @Override
	    public int getPartition(HComputeKey key, HComputeValue value, int numPartitions) {			
	    	int partition = 0;
	    	for (int i = 0; i < AIR_NAMES.length; ++i) {
	    		if (key.mAirName.equals(AIR_NAMES[i])) {
	    			partition = i;
	    			break;
	    		}
	    	}
	        return partition % numPartitions;
	    }
	}
	
	// The key grouping comparator "groups" values together according to the airline id. 	
	public static class HComputeGroupingComparator extends WritableComparator {

		public HComputeGroupingComparator() {
	        super(HComputeKey.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2){
	    	HComputeKey k1 = (HComputeKey) w1;
	    	HComputeKey k2 = (HComputeKey) w2;	    	
	        return k1.mAirName.compareTo(k2.mAirName);
	    }
	}

	public static class HComputeMapper extends TableMapper<HComputeKey, HComputeValue> {
		
		private class DelayInfo {
			public int 	  mCount;
			public double mDelay;
			
			public DelayInfo(int count, double delay) {
				mCount = count;
				mDelay = delay;
			}
		}
 
		// The map output key
		private HComputeKey mKey = new HComputeKey();
		
		// The map output value
		private HComputeValue mValue = new HComputeValue();
		
		// The hashmap used to accumulate the delay of each month of each airline
		private HashMap<String, HashMap<Integer, DelayInfo>> mAirlinesDelayMap = new HashMap<String, HashMap<Integer, DelayInfo>>();
		
		// The flight airline id
		public String mAirName;
		
		// The month for the flight (for secondary sort)
		public int mMonth;
		
		// The actual delay time of F1/F2 in minutes 
		public double mDelay;	
		
		@Override
	    public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
			// Parse the useful data
			process(row, values);
			
			// Get the delay map of the input airline if it's available, otherwise create a new one.								
			HashMap<Integer, DelayInfo> airlineDelayMap = mAirlinesDelayMap.get(mAirName);
			if (airlineDelayMap == null) {
				airlineDelayMap = new HashMap<Integer, DelayInfo>();
				for (int i = 1; i <= 12; ++i) {						
					airlineDelayMap.put(i, new DelayInfo(0, 0.0));
				}
				mAirlinesDelayMap.put(mAirName, airlineDelayMap);
			}
			
			// Accumulate the delay of the input month.
			DelayInfo info = airlineDelayMap.get(mMonth);
			info.mCount += 1;
			info.mDelay += mDelay;			
		}
		
		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Foreach airline
		    for (Entry<String, HashMap<Integer, DelayInfo>> airlineDelay : mAirlinesDelayMap.entrySet()) {
		    	String airName = airlineDelay.getKey();		    	
		    	HashMap<Integer, DelayInfo> airlineDelayMap = airlineDelay.getValue();

		    	// Foreach month of this airline
		    	for (Entry<Integer, DelayInfo> monthDelay : airlineDelayMap.entrySet()) {
		    		int month = monthDelay.getKey();
		    		DelayInfo info = monthDelay.getValue();
		    		
		    		// Emit the monthly total delay of this airline
					mKey.set(airName, month);
					mValue.set(month, info.mCount, info.mDelay);
					context.write(mKey, mValue);
		    	}
		    }
		}

		private boolean process(ImmutableBytesWritable row, Result result) {	
			// Get the cell from the column family:qualifier.
			int combo = Bytes.toInt(result.getColumnLatest(COLUMN_FAMILY, OTHER_QUALIFIER).getValue());
			
			// Parse the row key and the cell
			mMonth   = (int) row.get()[0];
			mAirName = AIR_NAMES[combo & 0xFF];
			mDelay   = combo >> 8;		
			
			return true;
		}
	}

	public static class HComputeReducer extends Reducer<HComputeKey, HComputeValue, Text, List<IntWritable>> {
		
		// The output key
		private Text mOutputKey = new Text();
		
		// The output value
		private List<IntWritable> mOutputValue = new ArrayList<IntWritable>(12);
		
		// This list is used to store the total and average delay time for each month
		private List<Double> mDelay = new ArrayList<Double>(12);
		
		// This list is used to store the total flight count for each month 
		private List<Integer> mCounter = new ArrayList<Integer>(12);
		
		/**
		 * Called once at the start of the task.
		 */
		protected void setup(Context context) throws IOException, InterruptedException {			
			for (int i = 0; i < 12; ++i) {
				mDelay.add(0.0);
				mCounter.add(0);
				mOutputValue.add(new IntWritable(0));
			}
		}
  
		public void reduce(HComputeKey key, Iterable<HComputeValue> values, Context context) 
				throws IOException, InterruptedException {
			// Reset the monthly delay list
			for (int i = 0; i < 12; ++i) {
				mDelay.set(i, 0.0);
				mCounter.set(i, 0);
			}
			
			// Get the total delay of each month
			for (HComputeValue value : values) {	
				Double delay = mDelay.get(value.mMonth - 1) + value.mDelay;
				mDelay.set(value.mMonth - 1, delay);
				Integer count = mCounter.get(value.mMonth - 1) + value.mCount;
				mCounter.set(value.mMonth - 1, count);
			}
			
			// Get the average delay of each month
			for (int i = 0; i < 12; ++i) {
				double average = mCounter.get(i) > 0 ? mDelay.get(i) / mCounter.get(i) : 0;				
				mDelay.set(i, (double) Math.ceil(average));
			}
			
			// Set the output key and value
			mOutputKey.set(key.mAirName);
			for (int i = 0; i < 12; ++i) {			
				mOutputValue.get(i).set(mDelay.get(i).intValue());
			}
			
			// Emit the average delay for this airline
			context.write(mOutputKey, mOutputValue);
		}		
	}
	
	public static class HComputerOutputFormat extends FileOutputFormat<Text, List<IntWritable>> {  
		  
	    private final String PREFIX = "average_delay_";
	    
	    @Override  
	    public RecordWriter<Text, List<IntWritable>> getRecordWriter(TaskAttemptContext context)  
	            throws IOException, InterruptedException {  
	        // Create a new writable file
	        Path outputDir = FileOutputFormat.getOutputPath(context);  
//		      	System.out.println("outputDir.getName():"+outputDir.getName()+",otuputDir.toString():"+outputDir.toString());  
	        String subfix = context.getTaskAttemptID().getTaskID().toString();  
	        Path filePath = new Path(outputDir.toString() + File.separator + PREFIX + subfix.substring(subfix.length() - 5, subfix.length()));  
	        FSDataOutputStream fileOut = filePath.getFileSystem(context.getConfiguration()).create(filePath);  
	        return new HComputerRecordWriter(fileOut);
	    }  
	} 
	
	public static class HComputerRecordWriter extends RecordWriter<Text, List<IntWritable>> {

	    private DataOutputStream mStream;

	    public HComputerRecordWriter(DataOutputStream stream) {
	    	mStream = stream;	       
	    }

	    @Override
	    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	    	mStream.close();
	    }

	    @Override
	    public void write(Text key, List<IntWritable> value) throws IOException, InterruptedException {
	        // Write out the key
	    	mStream.writeBytes(key.toString());
	    	
	        // Loop through all values associated with the key and write them with commas between
	        for (int i = 0; i < value.size(); ++i) {
            	mStream.writeBytes(", ");
	            mStream.writeBytes("(" + (i + 1) + ", " + value.get(i) + ")");
	        }
	        
	        mStream.writeBytes("\r\n");
	    }
	}

	static class JobRunner implements Runnable {
		
		private JobControl mControl;
	 
		public JobRunner(JobControl control) {
			mControl = control;
		}
	 
		public void run() {
			mControl.run();
		}
	}
	
	public static final String[] JOBNAMES = {"HCompute Job"}; 
	
	public static Job getJob(Configuration conf, String outputPath) throws IOException {
		Job job = new Job(conf, JOBNAMES[0]);
		job.setJarByClass(HCompute.class);

		// Configure mapper and reducer
		TableMapReduceUtil.initTableMapperJob(TABLE_NAME, buildScan(),
			      HComputeMapper.class, HComputeKey.class, HComputeValue.class, job);
		job.setPartitionerClass(HComputePartitioner.class);
		job.setGroupingComparatorClass(HComputeGroupingComparator.class);
		job.setReducerClass(HComputeReducer.class);
				
		// Configure output
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayList.class);
		job.setOutputFormatClass(HComputerOutputFormat.class);

		return job;
	}
		
	private static Scan buildScan() {
		Scan scan = new Scan();
	    
		// Get all columns
		scan.addFamily(COLUMN_FAMILY);
	    
		// Create the filter condition:
		// year is 0 (2007) or 1 (2008), cancel is 0 or 1.
		// cond = year << 1 | cancel, so cond should be 2.
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
			COLUMN_FAMILY, COND_QUALIFIER, CompareOp.EQUAL, new byte[] { (byte) 2 }
		);
		
		// Add the filter to the scan
		scan.setFilter(filter);
		
		// Disable Lru cache on region server
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		
		return scan;
	}
	
	public static void main(String[] args) throws Exception {		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			log.error("Usage: hcompute <out>");
			System.exit(2);
		}				
		
		// Delete the output if it does exist
		Path output = new Path(otherArgs[0]);
        boolean delete = output.getFileSystem(conf).delete(output, true);  
        if (delete) {
        	log.info("Deleted " + output + "? " + delete);
        }
		
		// Create "HCompute" job
		Job job = getJob(conf, otherArgs[0]);
		ControlledJob cJob = new ControlledJob(conf);
		cJob.setJob(job);
		
		// Create the job control.
		JobControl jobCtrl = new JobControl("jobctrl");
		jobCtrl.addJob(cJob);			    
		
		// Create a thread to run the job in the background.
		Thread jobRunnerThread = new Thread(new JobRunner(jobCtrl));
		jobRunnerThread.setDaemon(true);
		jobRunnerThread.start();

		while (!jobCtrl.allFinished()) {
			log.info("Still running...");
			Thread.sleep(5000);
		}
		log.info("Done");
		jobCtrl.stop();
 
		if (jobCtrl.getFailedJobList().size() > 0) {
			log.error(jobCtrl.getFailedJobList().size() + " jobs failed!");
			for (ControlledJob aJob : jobCtrl.getFailedJobList()) {
				log.error(aJob.getJobName() + " failed");
			}
			System.exit(1);
		} else {
			log.info("Success!! Workflow completed [" + jobCtrl.getSuccessfulJobList().size() + "] jobs.");
			System.exit(0);
		}
	}
}
