package edu.westga.cs4225.pagerank.model;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*Handles the sorting of the Page Ranks in ascending order*/
public class PageRankSorter {
	public static class Map_D extends MapReduceBase implements
			Mapper<LongWritable, Text, DoubleWritable, Text> {

		/*
		 * Sorts the list of pages from lowest to highest
		 */
		public void map(LongWritable key, Text value,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {
			int firstTab = value.find("\t");
			int secondTab = value.find("\t", firstTab + 1);

			String page = Text.decode(value.getBytes(), 0, firstTab);
			float pageRank = Float.parseFloat(Text.decode(value.getBytes(),
					firstTab + 1, secondTab - (firstTab + 1)));

			output.collect(new DoubleWritable(pageRank), new Text(page));
		}
	}
}
