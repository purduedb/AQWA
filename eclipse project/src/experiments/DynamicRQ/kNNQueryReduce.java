package experiments.DynamicRQ;

import helpers.FocalPoint;
import helpers.Tuple;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import experiments.DynamicRQ.kNNQueryMap.TupleAscComparer;

public class kNNQueryReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

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

	public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		while (values.hasNext()) {
			String line = values.next().toString();
			String [] tokens=line.split(",");

			try {
				double latitude = (Double.parseDouble(tokens[2]));
				double longitude = (Double.parseDouble(tokens[3]));

				Tuple t = new Tuple(longitude, latitude, line);
				t.setDistance(fp);
				if (queue.size() < k) {
					queue.add(t);					
				}
				else {
					if (t.distance < queue.peek().distance) {
						queue.add(t);
						queue.remove();
					}
				}

			} catch (Exception c) {

			}
		}

		while (!queue.isEmpty()) {
			output.collect(new Text(queue.remove().tupleData), new Text(""));			
		}
	}
}
