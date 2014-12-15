package com.binbo.secondary;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Secondary sort is a technique that allows the MapReduce programmer to control
 * the order that the values show up within a reduce function call.
 */
public class Secondary {

	// We only interest the average monthly flight delay of the year 2008.
	private final static int TARGET_YEAR = 2008;

	// Logger
	private static Logger log = Logger.getLogger(Secondary.class);

	public static class SecondaryKey implements WritableComparable<SecondaryKey> {

		// The flight airline id
		public String mAirName;

		// The month for the flight (for secondary sort)
		public int mMonth;

		public SecondaryKey() {
		}

		public void set(String airName, int month) {
			mMonth   = month;
			mAirName = airName;
		}

		// /////////////////////////////////////////// Implement Writable
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(mMonth);
			out.writeUTF(mAirName);
		}

		// /////////////////////////////////////////// Implement Writable
		@Override
		public void readFields(DataInput in) throws IOException {
			mMonth   = in.readInt();
			mAirName = in.readUTF();
		}

		// /////////////////////////////////////////// Implement Comparable
		@Override
		public int compareTo(SecondaryKey obj) {
			int result = mAirName.compareTo(obj.mAirName);
			if (result == 0) {
				result = mMonth - obj.mMonth;
			}
			return result;
		}
	}

	public static class SecondaryValue implements Writable {

		// The month for the flight
		public int mMonth;

		// The flight count
		public int mCount;

		// The actual monthly delay in minute
		public double mDelay;

		public SecondaryValue() {
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
	public static class SecondaryPartitioner extends
			Partitioner<SecondaryKey, SecondaryValue> {

		final static String[] mAirNames = { "9E", "AA", "AQ", "AS", "B6", "CO",
				"DL", "EV", "F9", "FL", "HA", "MQ", "NW", "OH", "OO", "UA",
				"US", "WN", "XE", "YV" };

		@Override
		public int getPartition(SecondaryKey key, SecondaryValue value,
				int numPartitions) {
			int partition = 0;
			for (int i = 0; i < mAirNames.length; ++i) {
				if (key.mAirName.equals(mAirNames[i])) {
					partition = i;
					break;
				}
			}
			return partition % numPartitions;
		}
	}

	// The key grouping comparator "groups" values together according to the
	// airline id.
	public static class SecondaryGroupingComparator extends WritableComparator {

		public SecondaryGroupingComparator() {
			super(SecondaryKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			SecondaryKey k1 = (SecondaryKey) w1;
			SecondaryKey k2 = (SecondaryKey) w2;
			return k1.mAirName.compareTo(k2.mAirName);
		}
	}

	public static class SecondaryMapper extends
			Mapper<Object, Text, SecondaryKey, SecondaryValue> {

		private class DelayInfo {
			public int    mCount;
			public double mDelay;

			public DelayInfo(int count, double delay) {
				mCount = count;
				mDelay = delay;
			}
		}

		// The map output key
		private SecondaryKey mKey = new SecondaryKey();

		// The map output value
		private SecondaryValue mValue = new SecondaryValue();

		// The CSV parser
		private CSVParser mParser = new CSVParser();

		// The hashmap used to accumulate the delay of each month of each
		// airline
		private HashMap<String, HashMap<Integer, DelayInfo>> mAirlinesDelayMap = new HashMap<String, HashMap<Integer, DelayInfo>>();

		// The flight airline id
		public String mAirName;

		// The month for the flight (for secondary sort)
		public int mMonth;

		// The actual delay time of F1/F2 in minutes
		public double mDelay;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			final String[] line = mParser.parseLine(value.toString());
			if (process(line)) {
				// Get the delay map of the input airline if it's available,
				// otherwise create a new one.
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
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// Foreach airline
			for (Entry<String, HashMap<Integer, DelayInfo>> airlineDelay : mAirlinesDelayMap
					.entrySet()) {
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

		private boolean process(String[] line) {
			// We only care flights of the year 2008.
			int year = Integer.parseInt(line[0].trim());
			if (year != TARGET_YEAR) {
				return false;
			}
			mMonth = Integer.parseInt(line[2].trim());
			if (mMonth < 1 || mMonth > 12) {
				return false;
			}

			// Neither of the two flights was cancelled.
			int cancelled = (int) Double.parseDouble(line[41].trim());
			if (cancelled == 1) {
				return false;
			}

			// Get useful data
			mAirName = line[6].trim();
			mDelay   = Double.parseDouble(line[37].trim());

			return true;
		}
	}

	public static class SecondaryReducer extends
			Reducer<SecondaryKey, SecondaryValue, Text, List<IntWritable>> {

		// The output key
		private Text mOutputKey = new Text();

		// The output value
		private List<IntWritable> mOutputValue = new ArrayList<IntWritable>(12);

		// This list is used to store the total and average delay time for each
		// month
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

		public void reduce(SecondaryKey key, Iterable<SecondaryValue> values, Context context)
				throws IOException, InterruptedException {
			// Reset the monthly delay list
			for (int i = 0; i < 12; ++i) {
				mDelay.set(i, 0.0);
				mCounter.set(i, 0);
			}

			// Get the total delay of each month
			for (SecondaryValue value : values) {
				Double delay = mDelay.get(value.mMonth - 1) + value.mDelay;
				mDelay.set(value.mMonth - 1, delay);
				Integer count = mCounter.get(value.mMonth - 1) + value.mCount;
				mCounter.set(value.mMonth - 1, count);
			}

			// Get the average delay of each month
			for (int i = 0; i < 12; ++i) {
				double average = mCounter.get(i) > 0 ? mDelay.get(i) / mCounter.get(i) : 0;
				mDelay.set(i, Math.ceil(average));
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

	public static class SecondaryOutputFormat extends FileOutputFormat<Text, List<IntWritable>> {

		private final String PREFIX = "average_delay_";

		@Override
		public RecordWriter<Text, List<IntWritable>> getRecordWriter(
				TaskAttemptContext context) throws IOException,
				InterruptedException {
			// Create a new writable file
			Path outputDir = FileOutputFormat.getOutputPath(context);
			// System.out.println("outputDir.getName():"+outputDir.getName()+",otuputDir.toString():"+outputDir.toString());
			String subfix = context.getTaskAttemptID().getTaskID().toString();
			Path filePath = new Path(outputDir.toString() + File.separator
					+ PREFIX + subfix.substring(subfix.length() - 5, subfix.length()));
			FSDataOutputStream fileOut = filePath.getFileSystem(
					context.getConfiguration()).create(filePath);
			return new SecondaryRecordWriter(fileOut);
		}
	}

	public static class SecondaryRecordWriter extends RecordWriter<Text, List<IntWritable>> {

		private DataOutputStream mStream;

		public SecondaryRecordWriter(DataOutputStream stream) {
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

			// Loop through all values associated with the key and write them
			// with commas between
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

	public static final String[] JOBNAMES = { "Secondary Job" };

	public static Job getJob(Configuration conf, String inputPath,
			String outputPath) throws IOException {
		Job job = new Job(conf, JOBNAMES[0]);
		job.setJarByClass(Secondary.class);

		// Configure input source
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Configure mapper and reducer
		job.setMapperClass(SecondaryMapper.class);
		job.setPartitionerClass(SecondaryPartitioner.class);
		job.setGroupingComparatorClass(SecondaryGroupingComparator.class);
		job.setReducerClass(SecondaryReducer.class);

		// Configure output
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapOutputKeyClass(SecondaryKey.class);
		job.setMapOutputValueClass(SecondaryValue.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayList.class);
		job.setOutputFormatClass(SecondaryOutputFormat.class);

		return job;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			log.error("Usage: secondary <in> <out>");
			System.exit(2);
		}

		// Delete the output if it does exist
		Path output = new Path(otherArgs[1]);
		boolean delete = output.getFileSystem(conf).delete(output, true);
		if (delete) {
			log.info("Deleted " + output + "? " + delete);
		}

		// Create "Secondary" job
		Job job = getJob(conf, otherArgs[0], otherArgs[1]);
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
			log.info("Success!! Workflow completed ["
					+ jobCtrl.getSuccessfulJobList().size() + "] jobs.");
			System.exit(0);
		}
	}
}
