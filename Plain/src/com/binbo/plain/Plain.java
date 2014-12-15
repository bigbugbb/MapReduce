package com.binbo.plain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

/*
 * Further optimization:
 * 1. F1 flights increasing order, F2 filghts decreasing order.
 * 2. Use flag also as part of the map key and retrieve F2 flighs only in list to save memory.
 */
public class Plain {
	
	public static class PlainLookUpKey implements WritableComparable<PlainLookUpKey> {
				
		// The flight date
		public String mDate;
		
		// The transit between ORD and JFK. It's the Dest column for F1 and the Origin column for F2. 
		public String mTransit;				
		
		// The actual arrival time of F1 or the actual departure time of F2 
		public String mTime;
		
		// The natural key which is the combination of mDate and mTransit
		public String mNaturalKey;

	    public PlainLookUpKey() {
	    }

	    public void set(String date, String transit, String time) {
	    	mDate    = date;
	    	mTransit = transit;
	    	mTime	 = time;	
	    	mNaturalKey = date + transit;
	    }

	    ///////////////////////////////////////////// Implement Writable
	    @Override
	    public void write(DataOutput out) throws IOException {
	    	out.writeUTF(mDate);
	    	out.writeUTF(mTransit);
	    	out.writeUTF(mTime);	    	
	    }

	    ///////////////////////////////////////////// Implement Writable
	    @Override
	    public void readFields(DataInput in) throws IOException {
	    	mDate    = in.readUTF();
	    	mTransit = in.readUTF();
	    	mTime	 = in.readUTF();	
	    	mNaturalKey = mDate + mTransit;
	    }

	    //////////////////////////////////////////// Implement Comparable
	    @Override
	    public int compareTo(PlainLookUpKey obj) {	        
	        return mNaturalKey.compareTo(obj.mNaturalKey);
	    }

	    @Override
	    public boolean equals(Object obj) {
	        if (obj == null || this.getClass() != obj.getClass()) { 
	        	return false;
	        }

	        PlainLookUpKey k = (PlainLookUpKey) obj;
	        if (k.mNaturalKey != null && mNaturalKey != null && !k.mNaturalKey.equals(mNaturalKey)) {
	        	return false;
	        }
	        return true;
	    }

	    @Override
	    public int hashCode() {
	        return mNaturalKey != null ? mNaturalKey.hashCode() : 0;
	    }

	    @Override
	    public String toString() {
	        return mDate + "," + mTransit + "," + mTime;
	    }
	}
	
	public static class PlainLookUpValue implements Writable {
		  
		// Tag to indicate whether it's F1 from ORD or F2 to JFK
		public int mTag;
		
		// The actual delay time of F1/F2 in minutes 
		public int mDelay;
		
		// The actual arrival time of F1 or the actual departure time of F2 
		public String mTime;
		
		public PlainLookUpValue() {			
		}
		
		public void set(int tag, int delay, String time) {
			mTag   = tag;
			mDelay = delay;
			mTime  = time;
		}

		public void write(DataOutput out) throws IOException {
		    out.writeInt(mTag);
		    out.writeInt(mDelay);
		    out.writeUTF(mTime);
		}

		public void readFields(DataInput in) throws IOException {
		    mTag   = in.readInt();
		    mDelay = in.readInt();
		    mTime  = in.readUTF();
		}

		public String toString() {
		    return mTag + "," + mDelay + "," + mTime;
		}
		
		@Override
		protected Object clone() throws CloneNotSupportedException {
			PlainLookUpValue cloned = new PlainLookUpValue();
			cloned.set(mTag, mDelay, mTime);
			return cloned;			
		}
	}

	// The composite key comparator is where the secondary sorting takes place. 
	// It compares composite key by PlainKey ascendingly, tag ascendingly and time ascendingly.
	public static class PlainLookForSortComparator extends WritableComparator {
		
	    public PlainLookForSortComparator() {
	        super(PlainLookUpKey.class, true);
	    }
	    
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	PlainLookUpKey k1 = (PlainLookUpKey) w1;
	    	PlainLookUpKey k2 = (PlainLookUpKey) w2;	         
	        int cmp = k1.compareTo(k2);
	        if (0 == cmp) {
        		cmp = k1.mTime.compareTo(k2.mTime);
	        }
	        return cmp;
	    }
	}
	
	// The key grouping comparator "groups" values together according to the natural key. 
	// Without this component, each K2={PlainKey, tag, time} and its associated V2=delay 
	// may go to different reducers.
	public static class PlainLookForGroupingComparator extends WritableComparator {

		public PlainLookForGroupingComparator() {
	        super(PlainLookUpKey.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2){
	    	PlainLookUpKey k1 = (PlainLookUpKey) w1;
	    	PlainLookUpKey k2 = (PlainLookUpKey) w2;	    	
	        return k1.compareTo(k2);
	    }
	}

	public static class PlainLookForMapper extends Mapper<Object, Text, PlainLookUpKey, PlainLookUpValue> {
 
		// The map output key
		private PlainLookUpKey mKey = new PlainLookUpKey();
		
		// The map output value
		private PlainLookUpValue mValue = new PlainLookUpValue();
		
		// The CSV parser
		private CSVParser mParser = new CSVParser();
		
		// The flight date
		private String mDate;
		
		// The transit between ORD and JFK. It's the Dest column for F1 and the Origin column for F2. 
		private String mTransit;
		
		// The actual arrival time of F1 or the actual departure time of F2 
		private String mTime;
		
		// The actual delay time of F1/F2 in minutes 
		private int mDelay;
		
		// Tag to indicate whether it's F1 from ORD or F2 to JFK
		private int mTag;
		   
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {			
			final String[] line = mParser.parseLine(value.toString());
			if (process(line)) {				
				mKey.set(mDate, mTransit, mTime);
				mValue.set(mTag, mDelay, mTime);
				context.write(mKey, mValue);
			}
		}
		
		private boolean process(String[] line) {
			// We only care all two-leg flights from airport ORD (Chicago) to JFK.
			String origin = line[11].trim();
			String dest   = line[17].trim();			
			if (origin.equals("ORD") && !dest.equals("JFK")) {
				mTag = 1;
			} else if (!origin.equals("ORD") && dest.equals("JFK")) {
				mTag = 2;
			} else {
				return false;
			}
			
			// Select only flights with a flight date between June 2007 and May 2008.
			int year = Integer.parseInt(line[0].trim());
			if (year < 2007 || year > 2008) {
				return false;
			}
			int month = Integer.parseInt(line[2].trim());
			if ((year == 2007 && month < 6) || (year == 2008 && month > 5)) {
				return false;
			}			
			
			// Neither of the two flights was cancelled or diverted.
			int cancelled = (int) Double.parseDouble(line[41].trim());
			int diverted  = (int) Double.parseDouble(line[43].trim());
			if (cancelled == 1 || diverted == 1) {
				return false;
			}
			
			// Get useful data only.
			mDate    = line[5].trim();
			mTransit = (mTag == 1) ? dest : origin;
			mTime	 = (mTag == 1) ? line[35].trim() : line[24].trim();
			mDelay   = (int) Double.parseDouble(line[37].trim());
			
			return true;
		}		
	}

	public static class PlainLookForReducer extends Reducer<PlainLookUpKey, PlainLookUpValue, Text, Text> {
		
		private List<PlainLookUpValue> mListF1 = new ArrayList<PlainLookUpValue>(32);
		private List<PlainLookUpValue> mListF2 = new ArrayList<PlainLookUpValue>(32);
		private List<PlainLookUpValue> mRemove = new ArrayList<PlainLookUpValue>(32);
		private int mSum   = 0;
		private int mCount = 0;		
		private Text k = new Text();
		private Text v = new Text();	
	
		public void reduce(PlainLookUpKey key, Iterable<PlainLookUpValue> values, Context context) 
				throws IOException, InterruptedException {						
			
			// Clear the vectors
			mListF1.clear();
			mListF2.clear();			
						
			// Split values into F1 list and F2 list
			for (PlainLookUpValue value : values) {	
				try {
					if (value.mTag == 1) {
						mListF1.add((PlainLookUpValue) value.clone());
					} else {
						mListF2.add((PlainLookUpValue) value.clone());
					}
				} catch (CloneNotSupportedException e) {
					e.printStackTrace();
				}
			}
			if (mListF1.isEmpty() || mListF2.isEmpty()) {
				return;
			}
			
			// The departure time of F2 is later than the arrival time of F1.		
			mRemove.clear();
			String minArrTime = mListF1.get(0).mTime; // the minimum arrival time 			
			for (PlainLookUpValue value : mListF2) {
				if (value.mTime.compareTo(minArrTime) <= 0) {					
					mRemove.add(value);
				}
			}
			mListF2.removeAll(mRemove);
			
			if (mListF2.isEmpty()) {
				return;
			}
			
			mRemove.clear();	
			String maxDepTime = mListF2.get(mListF2.size() - 1).mTime;
			for (PlainLookUpValue value : mListF1) {
				if (value.mTime.compareTo(maxDepTime) >= 0) {
					mRemove.add(value);
				}
			}
			mListF1.removeAll(mRemove);
			
			// Get all qualifying pairs (F1, F2) and their corresponding delay time			
			for (PlainLookUpValue value1 : mListF1) {
				for (PlainLookUpValue value2 : mListF2) {
					// The departure time of F2 is later than the arrival time of F1.
					if (value1.mTime.compareTo(value2.mTime) >= 0) {
						continue;
					}
					mCount += 1;
					mSum += value1.mDelay + value2.mDelay;
				}
			}										
		}
		
		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {			
			// Emit the count of two-leg delay and the sum of all delay time
			k.set(String.valueOf(mCount));
			v.set(String.valueOf(mSum));
			context.write(k, v);
		}
	}
	
	public static class PlainComputationValue implements Writable {
		  
		// The count of all delayed two-leg flights
		private int mCount;
		
		// The sum of all delay time
		private double mSum;
		
		public PlainComputationValue() {			
		}
		
		public void set(int count, double sum) {
			mCount = count;
			mSum   = sum;
		}

		public void write(DataOutput out) throws IOException {
		    out.writeInt(mCount);
		    out.writeDouble(mSum);
		}

		public void readFields(DataInput in) throws IOException {
			mCount = in.readInt();
			mSum   = in.readDouble();
		}

		public String toString() {
		    return mCount + "," + mSum;
		}		
	}
	
	public static class PlainComputationMapper extends Mapper<Object, Text, Text, PlainComputationValue> {
		 
		// The total count of delay two-leg flights
		private int mCount;
		
		// The sum of all delay time
		private double mSum;
		
		// The map output key
		private Text mKey = new Text();
		
		// The map output value
		private PlainComputationValue mValue = new PlainComputationValue();
		
		/**
		 * Called once at the beginning of the task.
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			mSum = mCount = 0;			
		}
		   
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException {		
			String[] splits = value.toString().split("\t");
			mCount += Integer.parseInt(splits[0].trim());
			mSum += Double.parseDouble(splits[1].trim());
		}		
						
		/**
		   * Called once at the end of the task.
		   */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Assume the count is larger than 0, which is reasonable for this problem.
			mKey.set("Average Delay");
			mValue.set(mCount, mSum);
			context.write(mKey, mValue);
		}
	}
	
	public static class PlainComputationReducer extends Reducer<Text, PlainComputationValue, Text, NullWritable> {

		// The final result
		private Text mAverage = new Text();

		public void reduce(Text key, Iterable<PlainComputationValue> values, Context context) 
				throws IOException, InterruptedException {   
			int count = 0;
			double sum = 0;
			for (PlainComputationValue value : values) {
				sum   += value.mSum;
				count += value.mCount;
			}
			mAverage.set(count + "\t" + sum + "\t" + sum / count);
			context.write(mAverage, NullWritable.get());
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
	
	public static final String[] JOBNAMES = {"PLAIN Look For Delay Pairs", "PLAIN Compute Delay Average"}; 
	
	public static Job getLookForJob(Configuration conf, String inputPath, String interDirPath) throws IOException {
		Job job = new Job(conf, JOBNAMES[0]);
		job.setJarByClass(Plain.class);

		// Configure output and input source
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Configure mapper and reducer
		job.setMapperClass(PlainLookForMapper.class);	
		job.setSortComparatorClass(PlainLookForSortComparator.class);
		job.setGroupingComparatorClass(PlainLookForGroupingComparator.class);
		job.setReducerClass(PlainLookForReducer.class);

		// Configure output
		FileOutputFormat.setOutputPath(job, new Path(interDirPath));
		job.setOutputKeyClass(PlainLookUpKey.class);
		job.setOutputValueClass(PlainLookUpValue.class);	

		return job;
	}
	
	public static Job getComputationJob(Configuration conf, String interDirPath, String outputDirPath) throws IOException {
		Job job = new Job(conf, JOBNAMES[1]);
		job.setJarByClass(Plain.class);
		
		// Configure output and input source
		FileInputFormat.addInputPath(job, new Path(interDirPath + "/part*"));
		
		// Configure mapper and reducer
		job.setMapperClass(PlainComputationMapper.class);	
		job.setReducerClass(PlainComputationReducer.class);
		job.setNumReduceTasks(1);
		
		// Configure output
		FileOutputFormat.setOutputPath(job, new Path(outputDirPath));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PlainComputationValue.class);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: plain <in> <temp> <out>");
			System.exit(2);
		}
		
		// Create "PLAIN Look For Delay Pairs" job
		Job job1 = getLookForJob(conf, otherArgs[0], otherArgs[1]);
		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1);
		
		// Create "PLAIN Compute Delay Average" job
		Job job2 = getComputationJob(conf, otherArgs[1], otherArgs[2]);
		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(job2);
		
		// Create the job control and add job dependency.
		JobControl jobCtrl = new JobControl("jobctrl");
		jobCtrl.addJob(cJob1);
		jobCtrl.addJob(cJob2);
		cJob2.addDependingJob(cJob1);
		
		// Create a thread to run the jobs in the background.
		Thread jobRunnerThread = new Thread(new JobRunner(jobCtrl));
		jobRunnerThread.setDaemon(true);
		jobRunnerThread.start();

		while (!jobCtrl.allFinished()) {
			System.out.println("Still running...");
			Thread.sleep(5000);
		}
		System.out.println("Done");				
		jobCtrl.stop();
 
		if (jobCtrl.getFailedJobList().size() > 0) {
			System.out.println(jobCtrl.getFailedJobList().size() + " jobs failed!");
			for (ControlledJob job : jobCtrl.getFailedJobList()) {
				System.out.println(job.getJobName() + " failed");
			}
			System.exit(1);
		} else {
			System.out.println("Success!! Workflow completed [" + jobCtrl.getSuccessfulJobList().size() + "] jobs.");
			System.exit(0);
		}
	}
}
