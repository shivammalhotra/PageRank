package app.job1;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WikiLinksMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static final Pattern wikiLinksPattern = Pattern.compile("\\[\\[(.+?)\\]\\]");
	public static Character[] ids = new Character[] { '#', ',', '.', '&', '\'', '-', '{' };

	public static List<Character> li = Arrays.asList(ids);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String title = getTitle(value);
		// if no page title found.
		if (title.equalsIgnoreCase("") || title.contains(":"))
			return;
		String newTitle = title.replace(' ', '_');
		newTitle = newTitle.substring(0, 1).toUpperCase() + newTitle.substring(1);
		Text pageTitle = new Text(newTitle);

		String linkstext = getText(value);
		// when there is no text inside <text></text> tag
		if (linkstext == "")
			return;
		Matcher matcher = wikiLinksPattern.matcher(linkstext);
		// extract all instances of wikilinks
		while (matcher.find()) {
			String outLink = matcher.group(1);
			// Filter only wiki pages.
			outLink = getWikiPageFromLink(outLink);
			if (outLink == null || outLink.isEmpty())
				continue;

			String newOutLink = outLink.substring(0, 1).toUpperCase() + outLink.substring(1);
			// add valid Pages to the map.
			context.write(new Text(newOutLink), pageTitle);
		}
		// put a special map value.
		context.write(pageTitle, new Text("!"));
	}

	public String getTitle(Text value) throws CharacterCodingException {
		int start = value.find("<title>");
		int end = value.find("</title>", start);
		if (start == -1 || end == -1)
			return "";
		start += 7; // add <title> length.

		return Text.decode(value.getBytes(), start, end - start);
	}

	public String getText(Text value) throws CharacterCodingException {
		int start = value.find("<text");
		start = value.find(">", start);
		int end = value.find("</text>", start);
		start += 1;

		if (start == -1 || end == -1)
			return "";

		return Text.decode(value.getBytes(), start, end - start);
	}

	public String getWikiPageFromLink(String linkText) {
		if (isNotWikiLink(linkText))
			return null;

		if (linkText.indexOf("|") > 0)
			linkText = linkText.substring(0, linkText.indexOf("|"));

		if (linkText.indexOf("#") > 0)
			linkText = linkText.substring(0, linkText.indexOf("#"));

		return linkText.replaceAll("\\s", "_");
	}

	public boolean isNotWikiLink(String linkText) {
		if (linkText.length() < 0 || linkText.length() > 100)
			return true;
		char firstChar = linkText.charAt(0);
		if (li.contains(firstChar))
			return true;
		if (linkText.contains(":"))
			return true;
		if (linkText.contains(","))
			return true;
		if (linkText.contains("&"))
			return true;

		return false;
	}

}