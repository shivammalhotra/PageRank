package app;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import app.job0.InitializePageRankMapper;
import  app.job0.InitializePageRankReducer;
import  app.job1.WikiLinksMapper;
import  app.job1.WikiLinksReducer;
import  app.job1.XmlInputFormat;
import  app.job2.OutGraphMapper;
import  app.job2.OutGraphReducer;
import  app.job3.CalcLinesMapper;
import  app.job3.CalcLinesReducer;
import  app.job4.CalcPageRankMapper;
import  app.job4.CalcPageRankReducer;
import  app.job5.OrderRankMapper;
import  app.job5.OrderRankReducer;

public class PageRank extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}

	@Override
	public int run(String[] arg0) throws Exception {

		// In and Out dirs in HDFS
		String inputXml = arg0[0];
		String outputDir = arg0[1];

		if (!parseXml(inputXml, outputDir + "wiki/ranking/iter0--â€�raw")) {
			return 1;
		}
		if (!getAdjacencyGraph("" + outputDir + "wiki/ranking/iter0--â€�raw", "" + outputDir + "wiki/ranking/iter0_1"))
			return 1;
		if (!calTotalPages(outputDir + "wiki/ranking/iter0_1", outputDir + "wiki/ranking/N"))
			return 1;
		int N = getCount(outputDir + "wiki/ranking/N");
		if (!initializePageRank(outputDir + "wiki/ranking/iter0_1", outputDir + "wiki/ranking/iter0", N))
			return 1;
		int i;
		for (i = 0; i < 8; i++) {
			calPageRank(outputDir + "wiki/ranking/iter" + i, outputDir + "wiki/ranking/iter" + (i + 1), N);
		}
		orderRank(outputDir + "wiki/ranking/iter1", outputDir + "wiki/ranking/pagerank1", N);
		orderRank(outputDir + "wiki/ranking/iter8", outputDir + "wiki/ranking/pagerank8", N);

		copyFile(outputDir + "wiki/ranking/iter0_1/part-r-00000", outputDir + "results/PageRank.outlink.out");
		copyFile(outputDir + "wiki/ranking/N/part-r-00000", outputDir + "results/PageRank.n.out");
		copyFile(outputDir + "wiki/ranking/pagerank1/part-r-00000", outputDir + "results/PageRank.iter1.out");
		copyFile(outputDir + "wiki/ranking/pagerank8/part-r-00000", outputDir + "results/PageRank.iter8.out");
		return 0;
	}

	public static void copyFile(String path_to_src, String path_to_dst) throws IOException {
		Path srcPath = new Path(path_to_src);
		Path dstPath = new Path(path_to_dst);
		Configuration conf = new Configuration();
		FileSystem fs_src = srcPath.getFileSystem(conf);
		FileSystem fs_dst = dstPath.getFileSystem(conf);
		FileUtil.copy(fs_src, srcPath, fs_dst, dstPath, false, conf);
	}

	public int getCount(String inputPath) {
		Configuration conf = new Configuration();
		String count;
		int noOfNodes = 0;
		try {
			FileSystem fs = FileSystem.get(new URI(inputPath + "/part-r-00000"), conf);
			Path inFile = new Path(new URI(inputPath + "/part-r-00000"));
			FSDataInputStream in = fs.open(inFile);
			while ((count = in.readLine()) != null) {
				noOfNodes = Integer.parseInt(count.split("=")[1].trim());
			}
			in.close();
			fs.close();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return noOfNodes;

	}

	public boolean parseXml(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

		Job job = Job.getInstance(conf, "job1");
		job.setJarByClass(PageRank.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapperClass(WikiLinksMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(WikiLinksReducer.class);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true);
	}

	public boolean getAdjacencyGraph(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "job2");
		job.setJarByClass(PageRank.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(OutGraphMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(OutGraphReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true);
	}

	public boolean calTotalPages(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job3");
		job.setJarByClass(PageRank.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(CalcLinesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(CalcLinesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true);
	}

	public boolean initializePageRank(String inputPath, String outputPath, int N) throws Exception {
		Configuration conf = new Configuration();
		conf.set("rank", String.valueOf(new Double(1.0 / N)));
		Job job = Job.getInstance(conf, "job");
		job.setJarByClass(PageRank.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		job.setMapperClass(InitializePageRankMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(InitializePageRankReducer.class);
		return job.waitForCompletion(true);
	}

	public boolean calPageRank(String inputPath, String outputPath, int N)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("N", String.valueOf(N));

		Job job = Job.getInstance(conf, "job4");
		job.setJarByClass(PageRank.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(CalcPageRankMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(CalcPageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true);
	}

	public boolean orderRank(String inputPath, String outputPath, int N)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("N", String.valueOf(N));

		Job job = Job.getInstance(conf, "job5");
		job.setJarByClass(PageRank.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(OrderRankMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(OrderRankReducer.OrderComparator.class);

		job.setReducerClass(OrderRankReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true);
	}
}