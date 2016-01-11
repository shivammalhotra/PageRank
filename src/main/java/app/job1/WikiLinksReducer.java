package app.job1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiLinksReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String inGraph = "";
		boolean first = true;
		boolean redlink = true;
		for (Text value : values) {
			if (value.toString().equalsIgnoreCase("!")) {
				redlink = false;
			} else {
				if (!first)
					inGraph += "\t";
				inGraph += value.toString();
				first = false;
			}
		}
		if (!redlink)
			context.write(key, new Text(inGraph));
	}
}