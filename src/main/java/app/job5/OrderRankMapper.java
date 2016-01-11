package app.job5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderRankMapper extends Mapper<Object, Text, DoubleWritable, Text> {

	private double minRank;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		minRank = 1.0 / Double.parseDouble(context.getConfiguration().get("N"));
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] nodes = value.toString().split("\t");
		double rank = Double.parseDouble(nodes[1]);
		String pageTitle = nodes[0];
		if (rank >= minRank)
			context.write(new DoubleWritable(rank), new Text(pageTitle));
	}
}
