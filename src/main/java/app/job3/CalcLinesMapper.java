package app.job3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalcLinesMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private Text word = new Text("N=");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		context.write(word, one);
	}
}