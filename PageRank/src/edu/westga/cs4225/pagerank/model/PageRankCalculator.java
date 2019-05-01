package edu.westga.cs4225.pagerank.model;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankCalculator {
	/*
	 * Reformats incoming data in two ways for the reducer Format 1: outlink
	 * numberoflinks Format 2: pagetitle |listoflinks
	 */
	public static class Map_D extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String values = value.toString();
			String[] data = values.split("\t");

			String page = data[0];
			String pageRank = data[1];
			String links = data[2];

			String[] outPages = links.split(",");

			for (String otherPage : outPages) {
				Text rankAndNumberOfLinks = new Text(pageRank + "\t"
						+ outPages.length);
				output.collect(new Text(otherPage), rankAndNumberOfLinks);
			}
			output.collect(new Text(page), new Text("|" + links));
		}
	}

	/*
	 * Calculates a pages page rank then reformats data to original input data
	 * form
	 */
	public static class Reduce_D extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String links = "";
			double sumShareOtherPageRanks = 0.0;

			while (values.hasNext()) {

				String content = values.next().toString();

				if (content.startsWith("|")) {
					links += content.substring(1);

				} else {

					String[] split = content.split("\\t");

					double pageRank = Double.parseDouble(split[0]);
					int totalLinks = Integer.parseInt(split[1]);

					sumShareOtherPageRanks += (pageRank / totalLinks);
				}

			}

			double newRank = .85 * sumShareOtherPageRanks + (1 - .85);
			if (!links.equals("")) {
				output.collect(key, new Text(newRank + "\t" + links));
			}

		}

	}
}
