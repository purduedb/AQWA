package partitioning;
import helpers.Constants;
import helpers.PartitionsInfo;
import index.RTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

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

import java.util.List;

import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.GreedyGR;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

public class PartitioningDriver {

	private static int k = 100;

	private static String partitions;

	// args[0] is the input dir, e.g., osm/all
	// args[1] is a flag that if contains "grid", we will create a grid
	// args[2] is k
	// arg[3] is the 
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
		k = Integer.parseInt(args[2]);
		String indexFolder = args[3];
		
		System.out.println("inputDir: " + inputDir);
		System.out.println("flag: " + flag);
		System.out.println("k: " + String.valueOf(k));
		System.out.println("indexFolder: " + indexFolder);
		
		if (flag.toLowerCase().contains("grid")) {
			System.out.println("");
			// This is the spatial hadoop baseline
			//gridPartitioning(args[0], "gridPartitions");
			gridPartitioning(inputDir, indexFolder);	
		}
		else {
			// These are the initial partiotins for the dynamic partitioning afterwards.
			// You have to redo this partitioning every time you repeat the experiment, e.g., for different k.
			// This can also be used as the static k-d tree which is one of the baseline
			//dynamicPartitioning(args[0], "dynamicPartitions");
			dynamicPartitioning(inputDir, indexFolder); 	
		}
	}

	private static void gridPartitioning(String inputDir, String outputDir) {
		Solution gridSol = new Solution();
		for (Partition p : getGridPartitions(k))
			gridSol.addPartition(p);		

		partitions = gridSol.toString();
		System.out.println(partitions);

		exec(inputDir, outputDir);
	}

	// initial partitions.
	private static void dynamicPartitioning(String inputDir, String outputDir) {
		CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
		Solution staticSolution = new Solution();
		DynamicPartitioning dynamic = new DynamicPartitioning(costEstimator, k, 1000);
		for (Partition p : dynamic.initialPartitions())
			staticSolution.addPartition(p);		

		partitions = staticSolution.toString();
		System.out.println(partitions);

		exec(inputDir, outputDir);
	}

	private static void exec(String inputDir, String outputDir) {
		Configuration mycon=new Configuration();
		
		JobConf conf = new JobConf(mycon, PartitioningDriver.class);
		try {
			Path op = new Path(outputDir);
			FileSystem fs = op.getFileSystem(conf);               
			fs.delete(op, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);

		conf.setMapperClass(DPMap.class);
		conf.setReducerClass(DPReduce.class);

		conf.setNumMapTasks(32);
		conf.setNumReduceTasks(32);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(DPMultipleOutput.class);

		conf.set("numRows", Integer.toString(Constants.gridHeight));
		conf.set("numColumns", Integer.toString(Constants.gridWidth));
		conf.set("minLat", Integer.toString(Constants.minLat));
		conf.set("minLong", Integer.toString(Constants.minLong));
		conf.set("maxLat", Integer.toString(Constants.maxLat));
		conf.set("maxLong", Integer.toString(Constants.maxLong));

		conf.set("partitions", partitions);

		//FileInputFormat.setInputPaths(conf,new);
		//FileInputFormat.addInputPath(conf, new Path(InputFiles));
		FileInputFormat.setInputPaths(conf, inputDir);
		FileOutputFormat.setOutputPath(conf,new Path(outputDir));
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Done Partitioning");
		PartitionsInfo.assertPartitioning(outputDir);
	}

	public static ArrayList<Partition> getGridPartitions(int k) {
		ArrayList<Partition> partitions = new ArrayList<Partition>();

		int numRows = (int)Math.sqrt(k);
		int numColumns = numRows;

		int cellWidth = Constants.gridWidth / numColumns;
		int cellHeight = Constants.gridHeight / numRows;

		for (int i = 0; ; i++) {
			int bottom = i * cellHeight;
			if (bottom >= Constants.gridHeight)
				break;
			int top = bottom + cellHeight;
			if (top > Constants.gridHeight)
				top = Constants.gridHeight;

			for (int j = 0; ; j++) {

				int left = j * cellWidth;
				if (left >= Constants.gridWidth)
					break;
				int right = left + cellWidth;
				if (right > Constants.gridWidth)
					right = Constants.gridWidth;

				Partition p = new Partition(bottom, top, left, right);
				partitions.add(p);				
			}
		}

		return partitions;
	}

}
