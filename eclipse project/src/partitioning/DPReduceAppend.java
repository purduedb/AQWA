package partitioning;

import helpers.Constants;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DPReduceAppend extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	String directoryPath;
	double cellHeight, cellWidth;
	
	public void configure(JobConf job) {
		directoryPath = job.get("appendDir");
		cellHeight = (Constants.maxLat - Constants.minLat)/ Constants.gridHeight;
		cellWidth = (Constants.maxLong - Constants.minLong) / Constants.gridWidth;		
	}
	
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		Path path = new Path(directoryPath + "/" + key.toString());
		
		try {
			if (!fs.exists(path)) {
				System.out.println("Path does not exist");
				output.collect(key, new Text("PATH DOES NOT EXIST"));
				return;
			}
			
			FSDataOutputStream fileOutputStream = fs.append(path);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
			
			HashMap<String, Integer> counts = new HashMap<String, Integer>();
			while (values.hasNext()) {
				String line = values.next().toString(); 
				
				// Count
				String [] tokens = line.split(",");
				int rowID = (int)((Double.parseDouble(tokens[2]) - Constants.minLat) / cellHeight);
				int columnID = (int)((Double.parseDouble(tokens[3]) - Constants.minLong) / cellWidth);
				if (rowID >= Constants.gridHeight || rowID < 0)
					return;
				if (columnID >= Constants.gridWidth || columnID < 0)
					return;
				String countKey = rowID + "," + columnID;
				Integer count = counts.get(countKey);
				if (count == null) {
					counts.put(countKey, 1);
				} else {
					counts.put(countKey, count + 1);
				}
				
				// Append
				bw.append(line + "\n");
			}
			
			bw.close();
			
			// Write counts
			for (String countKey : counts.keySet()) {
				output.collect(new Text(countKey), new Text(counts.get(countKey).toString()));
			}
			
		} catch (Exception e) {
			System.out.println(e);
		}
				
	}
}