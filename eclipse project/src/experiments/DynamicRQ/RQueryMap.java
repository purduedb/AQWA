package experiments.DynamicRQ;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class RQueryMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

	double minLat;
	double minLong;
	double maxLat;
	double maxLong;

	public void configure(JobConf job) {
		minLat = Double.parseDouble(job.get("minLat"));
		minLong = Double.parseDouble(job.get("minLong"));
		maxLat = Double.parseDouble(job.get("maxLat"));
		maxLong = Double.parseDouble(job.get("maxLong"));
	}

	public void map(LongWritable key, Text value,OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
		String [] tokens=value.toString().split(",");

		try {
			double latitude = (Double.parseDouble(tokens[2]));
			double longitude = (Double.parseDouble(tokens[3]));

			if (latitude >= minLat && latitude <= maxLat && longitude >= minLong && longitude <= maxLong) {
				
				output.collect(new Text("one"), new LongWritable(1));
			}
		} catch (Exception c) {

		}

	}
}