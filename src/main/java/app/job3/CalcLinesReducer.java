package app.job3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcLinesReducer extends Reducer<Text, IntWritable, Text, Text> {

	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable v : values)
			sum += v.get();
		result.set(sum);

		context.write(new Text("N=" + String.valueOf(result)), new Text());
	}
}