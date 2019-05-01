package edu.westga.cs4225.pagerank.driver;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.westga.cs4225.pagerank.model.LinkGraph;
import edu.westga.cs4225.pagerank.model.PageCount;
import edu.westga.cs4225.pagerank.model.PageRankCalculator;
import edu.westga.cs4225.pagerank.model.PageRankSorter;

public class Driver {

	public static String PATH_TO_DATA;
	public static int PageCount;
	private static String lastOutputPath;

	/*
	 * Entry point of program responsible for running each job
	 */
	public static void main(String[] args) throws Exception {

		getFilePath(args);
		runPageCounter();
		runLinkGrapher();
		runPageRankCalculatorTime(10);
		runSorter();
		
		printTopTen();
	}

	private static void printTopTen() throws IOException {
		ReversedLinesFileReader fr = new ReversedLinesFileReader(new File("Sorted/part-00000"));		
		for(int i = 0; i < 10; i++){
			System.out.println(i+1 + ". " + fr.readLine());
		}
		fr.close();
	}

	private static void runPageRankCalculatorTime(int iterations)
			throws IOException {
		System.out.println("Starting...");
		lastOutputPath = "";
		deleteOutputFolders("PageRank");
		String inFile = "LinkGraph";
		String outFile = "PageRank/iter01";
		runPageRankCalculator(inFile, outFile);

		for (int i = 1; i < iterations; i++) {
			inFile = "PageRank/iter" + new DecimalFormat("00").format(i);
			outFile = "PageRank/iter" + new DecimalFormat("00").format(i + 1);
			lastOutputPath = outFile;
			runPageRankCalculator(inFile, outFile);
		}
		System.out.println("Job3 Done!");
	}

	private static void getFilePath(String[] args) {
		try {
			PATH_TO_DATA = args[0];
		} catch (ArrayIndexOutOfBoundsException e) {
			Scanner scan = new Scanner(System.in);
			boolean fileDoesNotExists = true;
			while (fileDoesNotExists) {
				System.out.println("Please enter file path.");
				PATH_TO_DATA = scan.nextLine();
				File testFile = new File(PATH_TO_DATA);
				fileDoesNotExists = !testFile.exists();
			}
			scan.close();

		} catch (Exception e) {
			System.exit(1);
		}
	}

	private static void runSorter() throws IOException {
		System.out.println("Starting...");
		JobConf conf3 = new JobConf(PageRankSorter.class);
		conf3.setJobName("calculatePageRank");
		conf3.setOutputKeyClass(DoubleWritable.class);
		conf3.setOutputValueClass(Text.class);
		conf3.setMapperClass(PageRankSorter.Map_D.class);
		conf3.setInputFormat(TextInputFormat.class);
		conf3.setOutputFormat(TextOutputFormat.class);
		deleteOutputFolders("Sorted");

		FileInputFormat.setInputPaths(conf3, new Path(lastOutputPath));
		FileOutputFormat.setOutputPath(conf3, new Path("Sorted"));
		JobClient.runJob(conf3);

		System.out.println("Job4 Done!");
	}

	private static void runPageRankCalculator(String inFile, String outFile)
			throws IOException {

		JobConf conf2 = new JobConf(PageRankCalculator.class);
		conf2.setJobName("calculatePageRank");
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);
		conf2.setMapperClass(PageRankCalculator.Map_D.class);
		conf2.setReducerClass(PageRankCalculator.Reduce_D.class);
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf2, new Path(inFile));
		FileOutputFormat.setOutputPath(conf2, new Path(outFile));
		JobClient.runJob(conf2);

	}

	private static void runLinkGrapher() throws IOException {
		System.out.println("Starting...");
		JobConf conf1 = new JobConf(LinkGraph.class);
		conf1.setJobName("createLinkGraph");
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		conf1.setMapperClass(LinkGraph.Map_D.class);
		conf1.setReducerClass(LinkGraph.Reduce_D.class);
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);

		deleteOutputFolders("LinkGraph");

		FileInputFormat.setInputPaths(conf1, new Path(PATH_TO_DATA));
		FileOutputFormat.setOutputPath(conf1, new Path("LinkGraph"));
		JobClient.runJob(conf1);

		System.out.println("Job2 Done!");
	}

	private static void runPageCounter() throws IOException {
		System.out.println("Starting...");

		JobConf conf0 = new JobConf(PageCount.class);
		conf0.setJobName("pageCount");
		conf0.setOutputKeyClass(Text.class);
		conf0.setOutputValueClass(IntWritable.class);
		conf0.setMapperClass(PageCount.Map_D.class);
		conf0.setCombinerClass(PageCount.Reduce_D.class);
		conf0.setReducerClass(PageCount.Reduce_D.class);
		conf0.setInputFormat(TextInputFormat.class);
		conf0.setOutputFormat(TextOutputFormat.class);

		deleteOutputFolders("Total");

		FileInputFormat.setInputPaths(conf0, new Path(PATH_TO_DATA));
		FileOutputFormat.setOutputPath(conf0, new Path("Total"));
		JobClient.runJob(conf0);

		System.out.println("Job1 Done!");
	}

	private static void deleteOutputFolders(String fileName) {
		try {
			File f0 = new File(fileName);
			FileUtils.cleanDirectory(f0);
			FileUtils.forceDelete(f0);

		} catch (Exception e) {
		}
	}

}
