package app.job2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OutGraphMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] nodes = value.toString().split("\t");
		context.write(new Text(nodes[0]), new Text("!"));
		for (int i = 1; i < nodes.length; i++) {
			context.write(new Text(nodes[i]), new Text(nodes[0]));
		}
	}
}