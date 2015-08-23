package experiments.DynamicRQ;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

import helpers.FocalPoint;
import helpers.Tuple;

public class kNNQueryMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	FocalPoint fp;
	int k;

	PriorityQueue<Tuple> queue;

	public void configure(JobConf job) {
		double fpLat = Double.parseDouble(job.get("fpLat"));
		double fpLong = Double.parseDouble(job.get("fpLong"));
		fp = new FocalPoint(fpLong, fpLat);

		k = Integer.parseInt(job.get("k"));
		Comparator<Tuple> comparer = new TupleAscComparer();
		queue = new PriorityQueue<Tuple>(50, comparer);
	}

	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String [] tokens=value.toString().split(",");

		try {
			double latitude = (Double.parseDouble(tokens[2]));
			double longitude = (Double.parseDouble(tokens[3]));

			Tuple t = new Tuple(longitude, latitude, value.toString());
			t.setDistance(fp);
			if (queue.size() < k) {
				queue.add(t);
				output.collect(new Text("1"), value);
			}
			else {
				if (t.distance < queue.peek().distance) {
					queue.add(t);
					queue.remove();
					output.collect(new Text("1"), value);
				}
			}

		} catch (Exception c) {

		}

	}


	public static class TupleAscComparer implements Comparator<Tuple>{

		@Override
		public int compare(Tuple a, Tuple b) {

			if (a.distance < b.distance)
				return -1;
			if (a.distance > b.distance)
				return 1;

			return 0;
		}
	}

}