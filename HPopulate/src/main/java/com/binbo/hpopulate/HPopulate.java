package com.binbo.hpopulate;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVParser;

public class HPopulate {
	
	// Logger
	private static Logger log = Logger.getLogger(HPopulate.class);

	// HBase table name, column family and qualifers
	private final static String TABLE_NAME = "Flights";
	private final static byte[] COLUMN_FAMILY   = new byte[] { (byte) 'D' };
	private final static byte[] DELAY_QUALIFIER = new byte[] { (byte) 'd' };

	public static class HPopulateMapper extends Mapper<LongWritable, Text, Text, Text> {

		private HTable mTable;										
		
		private HashMap<String, Integer> mMap = new HashMap<String, Integer>(20);
		
		/**
		 * Called once at the beginning of the task.
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			// Create the HBase table client once up-front and keep it around
			// rather than create on each map invocation.
			try {
				mTable = new HTable(context.getConfiguration(), TABLE_NAME);
				mTable.setAutoFlush(false);
			} catch (IOException e) {
				throw new RuntimeException("Failed HTable construction", e);
			}		
			
			// Make the map from unique carrier name to index
			final String[] names = {
				"9E", "AA", "AQ", "AS", "B6", "CO", "DL", "EV", "F9", "FL", 
				"HA", "MQ", "NW", "OH", "OO", "UA", "US", "WN", "XE", "YV"
			};			
			for (int i = 0; i < names.length; ++i) {
				mMap.put(names[i], i);
			}
		}

		// The CSV parser
		private CSVParser mParser = new CSVParser();					
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Split the line
			final String[] line = mParser.parseLine(value.toString());
			
			// Parse the useful data						
			int year   = safeParseInt(line[0].trim()) - 2007;
			int month  = safeParseInt(line[2].trim());
			int cancel = (int) safeParseDouble(line[41].trim());
			int index  = mMap.get(line[6].trim());
			int delay  = (int) safeParseDouble(line[37].trim());
			
			// Insert a new record, use as less bytes as possible.
			Put put = new Put(getRowKey(year, cancel, month, index, key.get()));
			put.add(COLUMN_FAMILY, DELAY_QUALIFIER, new byte[] { (byte) (delay & 0xFF), (byte) (delay >> 8 & 0xFF) });			
			mTable.put(put);
		}
		
		private int safeParseInt(String s) {
			return s.isEmpty() ? 0 : Integer.parseInt(s);
		}
		
		private double safeParseDouble(String s) {			
			return s.isEmpty() ? 0.0 : Double.parseDouble(s);
		}
		
		private byte[] getRowKey(int year, int cancel, int month, int index, long offset) {
			// Construct the row key
			byte[] bytes = new byte[Bytes.SIZEOF_LONG];			
						
			// Use offset for uniqueness
			Bytes.putBytes(bytes, 0, Bytes.toBytes(offset), 0, Bytes.SIZEOF_LONG);
			
			// Year | Cancelled or not | Index of unique carrier name
			bytes[0] = (byte) (year << 1 | cancel);
			bytes[1] = (byte) month;
			bytes[2] = (byte) index;
			
			return bytes;
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {			
			mTable.close();
		}
	}

	public static boolean createHTable(Configuration conf, String tableName) throws IOException {
		boolean result = true;

		HTableDescriptor htd = new HTableDescriptor(tableName);
		htd.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
		
		log.info("Connecting");	
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			log.info("Creating Table");
			admin.createTable(htd);
			log.info("Done......");
		} catch (TableExistsException e) {
			log.warn(String.format("'%s' already exists", tableName));
		} catch (IOException e) {
			log.error(e.getMessage());
			result = false;
		} finally {
			if (admin != null) {
				admin.close();
			}
		}

		return result;
	}

	public static Job getJob(Configuration conf, String inputPath) throws IOException {
		Job job = new Job(conf, "HPOPULATE Job");
		job.setJarByClass(HPopulate.class);

		// Configure input source
		FileInputFormat.addInputPath(job, new Path(inputPath));

		// Configure mapper and reducer
		job.setMapperClass(HPopulateMapper.class);
		job.setNumReduceTasks(0);

		// Configure output
		job.setOutputFormatClass(NullOutputFormat.class);

		return job;
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

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			log.error("Usage: hpopulate <in> <wait_time>");
			System.exit(2);
		}
		
		log.info(String.format("Waiting for %s seconds for HBase setup done...", otherArgs[1]));
		Thread.sleep(Integer.parseInt(otherArgs[1]) * 1000);
		
		if (!createHTable(conf, TABLE_NAME)) {
			log.error(String.format("Fail to create table $s.", TABLE_NAME));
			System.exit(3);
		}

		// Create "Secondary" job
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
			log.info("Success!! Workflow completed ["
					+ jobCtrl.getSuccessfulJobList().size() + "] jobs.");
			System.exit(0);
		}
	}
}
