package core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 *
 * Applies a greedy partitioning scheme.
 * Generates general rectangular partitions, and hence the 'GR' in the class name.
 * 
 * @author aaly
 *
 */
public class GreedyGR extends Partitioner {
	//private final static Logger logger = Logger.getLogger(WorkloadCrawler.class.toString());

	/**
	 * Instantiates the greedy algorithm.
	 *  
	 * @param m The height of the search space.
	 * @param n The width of the search space.
	 * @param costEstimator This parameter is used when estimating the size and cost of the resulting partitions.
	 * @param minPartitionSize Minimum allowed partition size.
	 * @param maxPartitionSize Maximum allowed partition size.
	 */
	public GreedyGR(int m, int n, CostEstimator costEstimator, double minPartitionSize, double maxPartitionSize) {

		super(m, n, costEstimator, minPartitionSize, maxPartitionSize);
	}

	/**
	 * 
	 * Creates k partitions in a greedy fashion. The resulting partitions are general rectangular.
	 * It starts with the whole space as one partition. It precomputes the reduction in cost that can result in splitting a certain partition.
	 * Partitions are inserted in a priority queue based on this precomputed reduction cost. The top of the priority queue is the partiotion
	 * that if split will reduce the cost most. The method keeps splitting until k partitions are created. 
	 * 
	 * @param k The number of partitions required.
	 * @return An Arraylist of partitions.
	 */
	private ArrayList<Partition> splitBasedOnCost(int k) {
		Comparator<Cell> comparator = new CellCostComparator();
		PriorityQueue<Cell> pQueue = new PriorityQueue<Cell>(10, comparator);

		// Start with the whole space as one cell (root)
		Cell wholeSpace = new Cell(0, m, 0, n);
		wholeSpace.setCost(costEstimator.getCost(wholeSpace));
		chooseBestCostSplit(wholeSpace);
		pQueue.add(wholeSpace);

		int remainingPartitions = k - 1; // because we start with one cell, so it's a partition by itself
		ArrayList<Partition> partitions = new ArrayList<Partition>();

		// Continue splitting until:
		while (remainingPartitions > 0 && !pQueue.isEmpty()) {
			
			// Here we are sure that any more splitting will not reduce the cost anymore, so we break
			if (pQueue.peek().costReductionDueToSplit == 0)
				break;

			// If the split will result in partitions smaller than the block size in hadoop, do not do the splitting
			if (pQueue.peek().children[0].getSizeInBytes() < minPartitionSize ||
					pQueue.peek().children[1].getSizeInBytes() < minPartitionSize) {
				Cell partition = pQueue.remove();
				partitions.add(partition);
				continue;			
			}

			// Retrieve the cell that if split, will reduce the cost most (top of heap)
			Cell nextInHeap = pQueue.remove();

			// Compute the best position to split each child, without actually adding the corresponding partitions now to the output
			chooseBestCostSplit(nextInHeap.children[0]);
			chooseBestCostSplit(nextInHeap.children[1]);

			// Insert the splits into the heap
			pQueue.add(nextInHeap.children[0]);
			pQueue.add(nextInHeap.children[1]);

			remainingPartitions--;
		}

		// The priority queue will contain the remaining partitions		
		while (!pQueue.isEmpty())			
			partitions.add(pQueue.remove());				

		return partitions;
	}

	@Override
	public Solution partition(int k) {

		if (k < 2) {
			//logger.info("Number of partitions cannot be less than 2");
			return null;
		}
		if (k > n * m) {
			//logger.info("Number of partitions cannot be more than M x N (total number of users)");
			return null;
		}

		// First, split based on cost. Create the partitioning in a way that minimizes the overall execution time of the given the query load
		// Note: this phase may create less than k partitions. This case happens if at any point during the splitting process we are sure that any more partitioning will not reduce the cost (execution time related to the given query load)
		ArrayList<Partition> initialPartitions = splitBasedOnCost(k);
		
		// Then, after the above phase, some partitions may exceed the maximum allowed size of partitions. In this case, we split them further.
		// Note that after this phase we may end up with more than k partitions. There is an analytical proof that the number of partitions in the end will be at most 2k if proper parameters are set (in fact, max partition size should be total size/k).
		//Solution solution = Common.splitLargeCells(initialPartitions, minPartitionSize, maxPartitionSize, costEstimator);
		Solution solution = new Solution();
		for (Partition p : initialPartitions) {
			solution.addCost(p.getCost());
			solution.addPartition(p);
		}

		return solution;
	}

	/**
	 * 
	 * Precomputes the best splitting of a given partition.
	 * 
	 * @param parent The cell to which the precomputation is to be done.
	 */
	private void chooseBestCostSplit(Cell parent) {

		parent.costReductionDueToSplit = 0;
		Partition[] bestSplits = null;

		double parentCost = parent.getCost();
		
		// Try all horizontal splits		
		for (int i = parent.getBottom() + 1; i < parent.getTop(); i++) {			
			Partition[] splits = Common.split(parent, i, true, costEstimator);
			//if (i%100 == 0) {
				//System.out.println(i);
				//System.out.println(splits[0].getLeft() + ", " + splits[0].getRight() + ", " + splits[0].getBottom() + ", " + splits[0].getTop());
				//System.out.println(splits[1].getLeft() + ", " + splits[1].getRight() + ", " + splits[1].getBottom() + ", " + splits[1].getTop());
			//}
			
			double costReduction = parentCost - (splits[0].getCost() + splits[1].getCost());
			
			
			if (costReduction > parent.costReductionDueToSplit) {
				parent.costReductionDueToSplit = costReduction;
				bestSplits = splits;
			}
		}

		// Try all vertical splits
		for (int i = parent.getLeft() + 1; i < parent.getRight(); i++) {
			Partition[] splits = Common.split(parent, i, false, costEstimator);
	
			
			double costReduction = parentCost - (splits[0].getCost() + splits[1].getCost());
			if (costReduction > parent.costReductionDueToSplit) {
				parent.costReductionDueToSplit = costReduction;
				bestSplits = splits;
			}	
		}

		if (bestSplits != null) {
			parent.children[0] = new Cell(bestSplits[0].getBottom(), bestSplits[0].getTop(), bestSplits[0].getLeft(), bestSplits[0].getRight());
			parent.children[0].setCost(bestSplits[0].getCost());
			parent.children[0].setSizeInBytes(bestSplits[0].getSizeInBytes());
			parent.children[1] = new Cell(bestSplits[1].getBottom(), bestSplits[1].getTop(), bestSplits[1].getLeft(), bestSplits[1].getRight());
			parent.children[1].setCost(bestSplits[1].getCost());
			parent.children[1].setSizeInBytes(bestSplits[1].getSizeInBytes());
		}
	}

	/**
	 * 
	 * Represents a partition. The children of a cell are a precomputation of the best splitting that can occur to the cell.
	 * 
	 * @author aaly
	 *
	 */
	private class Cell extends Partition {
		public Cell(int bottom, int top, int left, int right) {
			super(bottom, top, left, right);
			this.children = new Cell[2];
		}				

		private Cell[] children;
		private double costReductionDueToSplit;		
	}

	/**
	 * Used in the priority queue to order the cells according to the reduction in cost they can achieve when split.
	 * @author aaly
	 *
	 */
	private class CellCostComparator implements Comparator<Cell>{
		@Override
		public int compare(Cell x, Cell y) {
			if (x.costReductionDueToSplit < y.costReductionDueToSplit)
				return 1;

			if (x.costReductionDueToSplit > y.costReductionDueToSplit)			
				return -1;

			return 0;
		}
	}

}
