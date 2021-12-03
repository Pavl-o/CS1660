import java.io.*;
import java.util.*;
import java.util.Map.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top5 {
	
	public static class Top5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private static HashSet<String> stopWordList;
	    private static HashMap<String, Integer> frequency;
		private Text word;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			stopWordList = new HashSet<String>(Arrays.asList("he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or"));
			frequency = new HashMap<String, Integer>();
			word = new Text();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				String str = word.toString();
				
				if (stopWordList.contains(str.toLowerCase())) { // so case is ignored when compared to all-lowercase stop words
					continue;
				}
				
				// keeps track of how many times we encounter each word
				if (frequency.containsKey(str)) {
					frequency.replace(str, frequency.get(str) + 1);
				} else {
					frequency.put(str, 1);
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			ArrayList<Entry<String, Integer>> count_arr = new ArrayList<Entry<String, Integer>>();
			
			// populate the above array of key-value pairs 
			for (HashMap.Entry<String, Integer> count: frequency.entrySet()) {
				count_arr.add(new AbstractMap.SimpleEntry<>(count.getKey(), count.getValue()));
			}

			// override the compare method's arguments so we can sort our key-value pairs
			Collections.sort(count_arr, new Comparator<Map.Entry<String, Integer>>() {
				public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
					return (e2.getValue()).compareTo(e1.getValue());
				}
			});
			
			// iterate over our top 5 that will be sent to the reducer
			// the iterator will start at the beginning of the already-sorted array
			int i = 0;
			for (Entry<String, Integer> count : count_arr) {
				context.write(new Text(count.getKey()), new IntWritable(count.getValue()));
				if (++i >= 5) { // hardcoded for this homework since we only need 5 
					break;
				}
			}
		}
	}
	
	public static class Top5Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private HashMap<String, Integer> frequency;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			frequency = new HashMap<String, Integer>();
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable num : values) {
				if (frequency.containsKey(key.toString())) {
					frequency.replace(key.toString(), frequency.get(key.toString()) + num.get());
				} else {
					frequency.put(key.toString(), num.get());
				}
			}
		}
	
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			ArrayList<Entry<String, Integer>> count_arr = new ArrayList<Entry<String, Integer>>();
			
			// populate the above array of key-value pairs
			for (HashMap.Entry<String, Integer> count: frequency.entrySet()) {
				count_arr.add(new AbstractMap.SimpleEntry<>(count.getKey(), count.getValue()));
			}
			
			// override the compare method's arguments so we can sort our top key-value pairs from mappers
			Collections.sort(count_arr, new Comparator<Map.Entry<String, Integer>>() {
				public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
					return (e2.getValue()).compareTo(e1.getValue());
				}
			});
			
			// iterate over our top 5s from each mapper that will be sent to the final output
			// the iterator will start at the beginning of the already-sorted array
			int i = 0;
			for (Entry<String, Integer> count : count_arr) {
				context.write(new Text(count.getKey()), new IntWritable(count.getValue()));
				if (++i >= 5) { // hardcoded for this homework since we only need 5 
					break;
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Top5 <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(Top5.class);
		job.setJobName("Top 5");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileInputFormat.setInputDirRecursive(job, true); // read files in folders (not currently used)
		
		job.setMapperClass(Top5Mapper.class);
		job.setReducerClass(Top5Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1); // only need 1 reducer since every mapper is only sending 5 pairs
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}