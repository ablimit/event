import java.io.IOException;
import java.util.*;
/*
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class parseXml {

  public static class Map extends
			Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// regxtry r = new regxtry(key.toString());

			String tmp = key.toString();
			if (tmp != null) {
				tmp = tmp.replaceAll("\\r", " ");
				tmp = tmp.replaceAll("\\n", " ");
				output.collect(new Text(tmp), value);
			}

		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(parseXml.class);
		conf.setJobName("wikiparse");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(StreamWikiDumpInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
