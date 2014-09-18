package partitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class DPMultipleOutput extends MultipleTextOutputFormat<Text, Text> {
	protected String generateFileNameForKeyValue(Text key, Text value, String name) {
		return key.toString();
	}
	
	protected Text generateActualKey(Text key, Text value) {
		return null;

	}
}
