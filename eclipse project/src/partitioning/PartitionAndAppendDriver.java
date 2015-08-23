package partitioning;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import core.Solution;

public class PartitionAndAppendDriver {

	public static void main(String[] args) {
	
//		String inItialInputPath = "/user/tqadah/tweet_us/tweet_us_2013_10_12.csv";
//		String indexPath = "growing_kd_200";
//		int initialK = 200;
//		
//		Solution layout = Common.initKDPartitioning(initialK);
//		System.out.println("Initialized kD-tree partitioning with layout: \n" + layout.toString());
//		
//		System.out.println("Initial partitioning");
//		Common.execPartitioning(inItialInputPath, indexPath, layout);
//		System.out.println("Done initial partitioning");
//		
//		String nextBatchPath = "/user/tqadah/tweet_us/tweet_us_2013_1_3.csv";
//		String countsPath = "append_output";
//		System.out.println("Appending data");
//		Common.execAppend(nextBatchPath, countsPath, indexPath, layout);
//		System.out.println("Done appending data");
	}

	
}
