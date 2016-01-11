package app.job4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcPageRankReducer extends Reducer<Text, Text, Text, Text> {
	private double d = 0.85;
	private int N;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		N = Integer.parseInt(context.getConfiguration().get("N"));
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double pageRank = 0.0;
		String outlinks = "";
		for (Text value : values) {
			String item = value.toString();
			if (item.startsWith("#")) {
				item = item.substring(1);
				outlinks += item;
			} else {
				pageRank += Double.parseDouble(item.trim());
			}
		}
		pageRank = d * pageRank;
		pageRank += (1 - d) / N;
		String output = "";
		output += String.valueOf(pageRank);
		if (outlinks.length() > 0) {
			output += "\t" + outlinks;
		}
		context.write(key, new Text(output));
	}
}