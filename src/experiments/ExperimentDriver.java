package experiments;

import org.apache.hadoop.util.ProgramDriver;

import partitioning.PartitioningDriver;

public class ExperimentDriver {

	public static void main(String argv[]){
		int ec = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("initialIndex",PartitioningDriver.class, "consturct intial paritioning");
			ec = pgd.run(argv);
		}
		catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(ec);

	}
	
}
