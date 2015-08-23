package partitioning;

import helpers.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

// Used to split a large file into multiple small files.

public class SplitData {

	// args[0] = input path
	// args[1] = output path
	// args[2] = numSplits
	public static void main(String[] args) {
		//String inItialInputPath = args[0];
		//String output = args[1];
		//int numFiles =  Integer.parseInt(args[2]);
		
		//split(inItialInputPath, output, numFiles);
		
		split("twitter_data", "splits", 5);
		
	}

	private static void split(String input, String output, int numFiles) {
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);

			Path inputPath = new Path(input);
			Path[] outputPath = new Path[numFiles];
			for (int i = 0; i < outputPath.length; i++) {
				outputPath[i] = new Path(output + "/" + i);
			}
			BufferedWriter[] bw = new BufferedWriter[numFiles];
			for (int i = 0; i < outputPath.length; i++) {
				bw[i] =  new BufferedWriter(new OutputStreamWriter(fs.create(outputPath[i])));
			}

			FileStatus[] files = fs.listStatus(inputPath);

			int lNumber = 0;
			for (FileStatus f : files) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));

				String line;
				line = br.readLine();
				while (line != null) {
					bw[lNumber % numFiles].write(line+"\n");
					lNumber++;
					line = br.readLine();
					if (lNumber %1000000 == 0) {
						System.out.println(lNumber/1000000 + " millions processed");
					}
				}
				br.close();
			}

			for (int i = 0; i < outputPath.length; i++) {
				bw[i].close();
			}
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
}
