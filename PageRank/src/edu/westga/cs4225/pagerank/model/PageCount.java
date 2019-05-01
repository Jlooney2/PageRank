package edu.westga.cs4225.pagerank.model;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.westga.cs4225.pagerank.driver.Driver;

/*
 * Handles getting the number of pages in the file.
 */
public class PageCount {
	/*
	 * Gets all of the pages and formats them for the reducer.
	 */
	public static class Map_D extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text doc = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			int title_start = line.indexOf("<title>");
			int title_end = line.indexOf("</title>");
			if (!line.substring(title_start, title_end).contains(":")) {
				output.collect(doc, one);
			}
		}
	}

	/*
	 * Calculates the total number of pages in the file
	 */
	public static class Reduce_D extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int total = 0;
			while (values.hasNext()) {
				total += values.next().get();
			}
			Driver.PageCount = total;
			output.collect(key, new IntWritable(total));
		}
	}
}