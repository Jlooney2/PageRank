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

import edu.westga.cs4225.pagerank.driver.Driver;

/*
 * Builds a graph of links that are on a page
 */
public class LinkGraph {
	/*
	 * Parses the line to retrieve the title of the page and all the links that
	 * for the given page then formats the data for the reducer Format:
	 * pagetitle outlink
	 */
	public static class Map_D extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private static final int TEXT_TAG = 6;
		private static final int TITLE_TAG = 7;
		private Text title = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			int title_start = line.indexOf("<title>");
			int title_end = line.indexOf("</title>");

			int text_start = line.indexOf("<text>");
			int text_end = line.indexOf("</text>");
			String pageContents = line.substring(text_start + TEXT_TAG,
					text_end);

			if (!line.substring(title_start, title_end).contains(":")) {
				title.set(line.substring(title_start + TITLE_TAG, title_end)
						.trim());

				getLinks(output, pageContents);

			}
		}

		private void getLinks(OutputCollector<Text, Text> output,
				String pageContents) throws IOException {
			int openCount = 0;
			int innerOpenCount = 0;

			String link = "";
			String innerLink = "";

			for (int i = 0; i < pageContents.length(); i++) {
				char curr = pageContents.charAt(i);
				if (curr == '[' && openCount != 2) {
					openCount++;
				} else if (curr == '[' && openCount == 2 && innerOpenCount != 2) {
					innerOpenCount++;
				} else if (curr == ']' && innerOpenCount == 2) {
					if (!innerLink.contains(":")) {
						if (innerLink.contains("|")) {
							int pipeIndex = innerLink.indexOf("|");
							innerLink = innerLink.substring(0, pipeIndex);
						}
						if (!innerLink.equals("")) {
							output.collect(title, new Text(innerLink.trim()));
						}

					}

					innerLink = "";
					innerOpenCount = 0;
					i++;
				} else if (curr == ']') {
					if (!link.contains(":")) {
						if (link.contains("|")) {
							int pipeIndex = link.indexOf("|");
							link = link.substring(0, pipeIndex);
						}
						if (!link.equals("")) {
							output.collect(title, new Text(link.trim()));
						}

					}
					link = "";
					openCount = 0;
					i++;
				} else if (innerOpenCount == 2) {
					innerLink += curr;
				} else if (openCount == 2) {
					link += curr;
				}
			}
		}
	}

	/*
	 * Combines the lines from the mapper, adds initial rank and reformats the
	 * data for the next job Format: pagetitle pagerank listoflinks
	 */
	public static class Reduce_D extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			boolean first = true;
			double initialRank = .85 / Driver.PageCount;
			initialRank = (initialRank * 100);
			String links = initialRank + "\t";
			while (values.hasNext()) {
				Text nextLink = values.next();
				if (first) {
					links += nextLink;
				}

				if (!links.contains(nextLink.toString()) && !first) {
					links += ",";
					links += nextLink;
				}

				first = false;
			}

			output.collect(key, new Text(links));
		}

	}

}
