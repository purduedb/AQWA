package helpers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GetBounds {

	public static void main(String[] args) {
		// args[0] should be the path to the folder containing the data.
		// Works fine when the data is in HDFS.

		System.out.println("Starting Processing");
		double minLat = 1000;
		double maxLat = -1000;
		double minLong = 1000;
		double maxLong = -1000;

		String line = null;
		int i = 0;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			Path path=new Path(args[0]);
			System.out.println(path.toString());
			FileStatus[] files = fs.listStatus(path);
			System.out.println(files.length + " files exist");

			for (FileStatus f : files) {
				System.out.println(f.getPath().toString());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));

				line = br.readLine();
				while (line != null) {

					if (++i%1000000 == 0)
						System.out.println(i/1000000 + " millions processed");

					String[] parts = line.split(",");

					try {
						double latitude =  Double.parseDouble(parts[2]);
						double longitude = Double.parseDouble(parts[3]);
						if (latitude < minLat)
							minLat = latitude;
						if (latitude > maxLat)
							maxLat = latitude;
						if (longitude < minLong)
							minLong = longitude;
						if (longitude > maxLong)
							maxLong = longitude;
					} catch(Exception c) {						
						//System.out.println("Exception happened at Tweet " + i + ": " + line);
					} finally {
						line = br.readLine();
					}					
				}

				br.close();
			}

			System.out.println("Done Processing");

			System.out.println("Min Latitude = " + minLat);
			System.out.println("Max Latitude = " + maxLat);
			System.out.println("Min Longitude = " + minLong);
			System.out.println("Max Longitude = " + maxLong);
		} catch (Exception e) {
			System.out.println("Exception" + e);
			System.out.println("Exception happened at Tweet " + i + ": " + line);
		}
	}	

}
