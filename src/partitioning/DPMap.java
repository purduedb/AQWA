package partitioning;

import helpers.PartitionsInfo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.turn.platform.cheetah.partitioning.horizontal.Partition;



public class DPMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	PartitionsInfo partitionsInfo;
	public void configure(JobConf job) {
		partitionsInfo = new PartitionsInfo(job);
	}

	public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Partition partition = partitionsInfo.getPartitionID(value.toString());

		Text word = new Text();
		word.set(partition.getBottom() + "," +partition.getTop() + "," + partition.getLeft() + "," + partition.getRight());
		//System.out.println(word);
		output.collect(word, value);
	}

}
