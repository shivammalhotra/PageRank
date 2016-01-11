package app.job0;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitializePageRankMapper extends Mapper<Object, Text, Text, Text> {
	private Double rank;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		rank = Double.parseDouble(context.getConfiguration().get("rank"));
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] nodes = value.toString().split("\t");
		String list_with_rank = "";
		list_with_rank += rank.toString();
		for (int i = 1; i < nodes.length; i++) {
			list_with_rank += "\t" + nodes[i];
		}
		context.write(new Text(nodes[0]), new Text(list_with_rank));
	}
}