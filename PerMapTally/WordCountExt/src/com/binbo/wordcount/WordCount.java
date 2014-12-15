/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package com.binbo.wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable> {	  		
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {          
      StringTokenizer itr = new StringTokenizer(value.toString());
      HashMap<Text, IntWritable> wordMap = new HashMap<Text, IntWritable>(); 
      while (itr.hasMoreTokens()) {
    	final String token = itr.nextToken();    	    	
    	// We only want to count real words starting with letters 
    	// "m", "n", "o", "p", and "q" (no matter if they are capitalized or not)
        int c = token.charAt(0);
        if (c == 'm' || c == 'M' || c == 'n' || c == 'N' || c == 'o' || c == 'O' || 
            c == 'p' || c == 'P' || c == 'q' || c == 'Q') {
          Text word = new Text(token);
          if (wordMap.containsKey(word)) {
        	IntWritable count = wordMap.get(word); 
        	count.set(count.get() + 1);
        	wordMap.put(word, count);        		
          } else {
        	wordMap.put(word, new IntWritable(1));
          }
        }
      }
      
      for (Map.Entry<Text, IntWritable> entry : wordMap.entrySet()) {
    	context.write(entry.getKey(), entry.getValue());
      }
    }
  }
  
  // The custom partitioner to this Word Count program
  public static class WordPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
	  // this is done to avoid performing mod with 0
      if (numPartitions == 0) {
        return 0;
      }
        
      // Assign words starting with "M" or "m" to reduce task 0, 
      // those starting with "N" or "n" to task 1, and so on.
      int target = 0;
      switch (key.charAt(0)) {
      case 'm': 
      case 'M':
        target = 0; 
        break;
      case 'n': 
      case 'N':
       	target = 1 % numPartitions; 
       	break;
      case 'o': 
      case 'O':        
       	target = 2 % numPartitions; 
       	break;
      case 'p':
      case 'P':
       	target = 3 % numPartitions;
       	break;
      case 'q':
      case 'Q':
       	target = 4 % numPartitions;
       	break;
      }
        
      return target;
	}	  
  }
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable mResult = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      mResult.set(sum);
      context.write(key, mResult);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class); // Disable the combiner
    job.setPartitionerClass(WordPartitioner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
