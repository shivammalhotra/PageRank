package app.job4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalcPageRankMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] nodes = value.toString().split("\t");
		Text pageTitle = new Text(nodes[0]);
		double rank = Double.parseDouble(nodes[1]);

		String output = "#";
		int noOfOutlinks = 0;
		for (int i = 2; i < nodes.length; i++) {
			if (!nodes[i].equalsIgnoreCase("")) {
				noOfOutlinks++;
			}
		}
		if (noOfOutlinks > 0) {
			rank /= noOfOutlinks;
		}
		for (int i = 2; i < nodes.length; i++) {
			if (!nodes[i].equalsIgnoreCase("")) {
				output += "\t" + nodes[i];
				context.write(new Text(nodes[i]), new Text(String.valueOf(rank)));
			}
		}

		context.write(pageTitle, new Text(output));
	}
}