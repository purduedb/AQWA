package partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import partitioning.*;
import helpers.Constants;

import java.util.List;

import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.GreedyGR;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

public class PartitioningDriver {

	// args[0] is the input dir, e.g., osm/all
	// args[1] is a flag that if contains "grid", we will create a grid
	// args[2] is k
	// arg[3] is the index folder
	public static void main(String[] args) {

		if (args.length < 4) {
			System.err.println("Invalid parameters.");
		}

		int i = 0;
		for(String s : args) {
			System.out.println("param " + String.valueOf(i) + ":" + s);			
			i++;
		}
		String inputDir = args[0];
		String flag = args[1];
		int k = Integer.parseInt(args[2]);
		String indexFolder = args[3];

		System.out.println("inputDir: " + inputDir);
		System.out.println("flag: " + flag);
		System.out.println("k: " + String.valueOf(k));
		System.out.println("indexFolder: " + indexFolder);

		Solution layout;
		if (flag.toLowerCase().contains("grid")) {
			System.out.println("");
			// This is the spatial hadoop baseline
			layout = Common.initGridPartitioning(k);	
		}
		else {
			// These are the initial partitions for the dynamic partitioning afterwards.
			// You have to redo this partitioning every time you repeat the experiment, e.g., for different k.
			// This can also be used as the static k-d tree which is one of the baseline
			String initialCounts = "Twitter0Counts";
			CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
			costEstimator.updateCountsInGrid(initialCounts);
			layout = Common.initKDPartitioning(k, costEstimator);			
		}
		Common.execPartitioning(inputDir, indexFolder, layout);
	}
	
}
