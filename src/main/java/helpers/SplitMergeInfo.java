package helpers;

import com.turn.platform.cheetah.partitioning.horizontal.Partition;

public class SplitMergeInfo {

	public Partition splitParent;
	public Partition splitChild1;
	public Partition splitChild0;
	
	public Partition mergeParent;
	public Partition mergeChild1;
	public Partition mergeChild0;
	
	public double estimateExecCost;
	public double optimalExecCost;
}
