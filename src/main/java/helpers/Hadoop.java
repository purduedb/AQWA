package helpers;

import org.apache.hadoop.conf.Configuration;

public class Hadoop {
	
	public static Configuration getConfiguration() {
		Configuration res = new Configuration();
		res.set("defaultFS", "tachyon://");
		return res;
	}

}
