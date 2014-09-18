package com.turn.platform.cheetah.partitioning.horizontal;

import helpers.SplitMergeInfo;
import index.RTree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;


public class DynamicPartitioning {

	private PriorityQueue<Cell> minHeap;
	private PriorityQueue<Cell> maxHeap;

	private CostEstimator costEstimator;
	private int k;

	public RTree<Cell> partitionsRTree;
	public ArrayList<Partition> currentPartitions;

	public SplitMergeInfo processNewQuery(Partition newQuery) {
		
		
		costEstimator.processNewQuery(newQuery);
		List<Cell> overlappingPartitions = partitionsRTree.searchExclusive(newQuery.getCoords(), newQuery.getDimensions());
		for (Cell c : overlappingPartitions) {
			chooseBestCostSplit(c);
			
			maxHeap.remove(c);
			maxHeap.add(c);

			if (minHeap.contains(c.parent)) {
				c.parent.costIncreaseDueToMerge = 2 * costEstimator.getSize(c.parent) + costEstimator.getCost(c.parent) - (costEstimator.getCost(c.parent.children[0]) + costEstimator.getCost(c.parent.children[1]));
				//c.parent.costIncreaseDueToMerge = costEstimator.getCost(c.parent) - (costEstimator.getCost(c.parent.children[0]) + costEstimator.getCost(c.parent.children[1]));
				minHeap.remove(c.parent);
				minHeap.add(c.parent);
			}			
		}

		if (minHeap.peek().costIncreaseDueToMerge < maxHeap.peek().costReductionDueToSplit) {

			SplitMergeInfo splitMergeInfo = new SplitMergeInfo();
			
			// Removing
			Cell toBeMerged = minHeap.remove();
			splitMergeInfo.mergeParent = toBeMerged;
			splitMergeInfo.mergeChild0 = toBeMerged.children[0];
			splitMergeInfo.mergeChild1 = toBeMerged.children[1];
			
			//toBeMerged.parent.costIncreaseDueToMerge = 2 * costEstimator.getSize(toBeMerged.parent) + costEstimator.getCost(toBeMerged.parent) - (costEstimator.getCost(toBeMerged.parent.children[0]) + costEstimator.getCost(toBeMerged.parent.children[1]));
			toBeMerged.parent.costIncreaseDueToMerge = costEstimator.getCost(toBeMerged.parent) - (costEstimator.getCost(toBeMerged.parent.children[0]) + costEstimator.getCost(toBeMerged.parent.children[1]));
			currentPartitions.add(toBeMerged);						
			if (currentPartitions.contains(toBeMerged.parent.children[0]) && currentPartitions.contains(toBeMerged.parent.children[1]))
				minHeap.add(toBeMerged.parent);

			maxHeap.remove(toBeMerged.children[0]);
			maxHeap.remove(toBeMerged.children[1]);				

			currentPartitions.remove(toBeMerged.children[0]);
			currentPartitions.remove(toBeMerged.children[1]);

			double[] coords = new double[2];

			coords[0] = (double)(toBeMerged.children[0].getLeft() + toBeMerged.children[0].getRight())/2;
			coords[1] = (double)(toBeMerged.children[0].getBottom() + toBeMerged.children[0].getTop())/2;
			partitionsRTree.delete(coords, toBeMerged.children[0]);

			
			coords[0] = (double)(toBeMerged.children[1].getLeft() + toBeMerged.children[1].getRight())/2;
			coords[1] = (double)(toBeMerged.children[1].getBottom() + toBeMerged.children[1].getTop())/2;
			partitionsRTree.delete(coords, toBeMerged.children[1]);			
			
			chooseBestCostSplit(toBeMerged);
			maxHeap.add(toBeMerged);
			partitionsRTree.insert(toBeMerged.getCoords(), toBeMerged.getDimensions(), toBeMerged);
			
			

			// Adding
			Cell toBeSplit = maxHeap.remove();			

			splitMergeInfo.splitParent = toBeSplit;
			splitMergeInfo.splitChild0 = toBeSplit.children[0];
			splitMergeInfo.splitChild1 = toBeSplit.children[1];
			
			minHeap.remove(toBeSplit.parent);
			coords[0] = (double)(toBeSplit.getLeft() + toBeSplit.getRight())/2;
			coords[1] = (double)(toBeSplit.getBottom() + toBeSplit.getTop())/2;
			partitionsRTree.delete(coords, toBeSplit);
			currentPartitions.remove(toBeSplit);

			chooseBestCostSplit(toBeSplit.children[0]);
			chooseBestCostSplit(toBeSplit.children[1]);
			maxHeap.add(toBeSplit.children[0]);
			maxHeap.add(toBeSplit.children[1]);
			currentPartitions.add(toBeSplit.children[0]);
			currentPartitions.add(toBeSplit.children[1]);

			partitionsRTree.insert(toBeSplit.children[0].getCoords(), toBeSplit.children[0].getDimensions(), toBeSplit.children[0]);
			partitionsRTree.insert(toBeSplit.children[1].getCoords(), toBeSplit.children[1].getDimensions(), toBeSplit.children[1]);			

			toBeSplit.costIncreaseDueToMerge = 2 * costEstimator.getSize(toBeSplit) + costEstimator.getCost(toBeSplit) - (costEstimator.getCost(toBeSplit.children[0]) + costEstimator.getCost(toBeSplit.children[1]));
			minHeap.add(toBeSplit);
			
			return splitMergeInfo;
		}
		
		return null;
	}

	public DynamicPartitioning(CostEstimator costEstimator, int k) {
		this.costEstimator = costEstimator;
		this.k = k;
	}

	public ArrayList<Partition> initialPartitions() {
		Comparator<Cell> comparator = new MaxHeapComparator();
		PriorityQueue<Cell> pQueue = new PriorityQueue<Cell>(10, comparator);

		// Start with the whole space as one cell (root)
		Cell wholeSpace = new Cell(0, costEstimator.getNumRows(), 0, costEstimator.getNumColumns());
		wholeSpace.setCost(costEstimator.getNumberPoints(wholeSpace));
		chooseBestSizeSplit(wholeSpace);
		pQueue.add(wholeSpace);

		int remainingPartitions = k - 1; // because we start with one cell, so it's a partition by itself
		ArrayList<Partition> partitions = new ArrayList<Partition>();

		// Continue splitting until:
		while (remainingPartitions > 0) {

			// Retrieve the cell that if split, will reduce the cost most (top of heap)
			Cell nextInHeap = pQueue.remove();

			if (nextInHeap.children[0] == null) {
				//pQueue.add(nextInHeap);
				//System.out.println("Warning k could not be reached; only " + (k-remainingPartitions) + "done");
				//break;
				partitions.add(nextInHeap);
				//remainingPartitions--;
				continue;
			}

			// Compute the best position to split each child, without actually adding the corresponding partitions now to the output
			chooseBestSizeSplit(nextInHeap.children[0]);
			chooseBestSizeSplit(nextInHeap.children[1]);

			// Insert the splits into the heap
			pQueue.add(nextInHeap.children[0]);
			pQueue.add(nextInHeap.children[1]);

			remainingPartitions--;
		}

		// The priority queue will contain the remaining partitions		
		while (!pQueue.isEmpty())			
			partitions.add(pQueue.remove());				

		initStructures(partitions);

		return partitions;
	}

	private void initStructures(ArrayList<Partition> partitions) {
		Comparator<Cell> maxComparator = new MaxHeapComparator();
		Comparator<Cell> minComparator = new MinHeapComparator();
		minHeap = new PriorityQueue<Cell>(10, minComparator);
		maxHeap = new PriorityQueue<Cell>(10, maxComparator);

		partitionsRTree = new RTree<Cell>(10, 5, 2);	
		for (Partition p : partitions) {
			chooseBestCostSplit((Cell)p);
			((Cell)p).costReductionDueToSplit = -2 * costEstimator.getSize(p); // 0 if we ignore the cost of repartitioning
			maxHeap.add((Cell)p);

			if (!minHeap.contains(((Cell)p).parent)) {
				if (partitions.contains(((Cell)p).parent.children[0]) &&
						partitions.contains(((Cell)p).parent.children[1])) {						
					((Cell)p).parent.costIncreaseDueToMerge = 2 * costEstimator.getSize(p); // 0 if we ignore the cost of repartitioning
					minHeap.add(((Cell)p).parent);	
				}
			}
			partitionsRTree.insert(p.getCoords(), p.getDimensions(), (Cell)p);
		}
		this.currentPartitions = partitions; 
	}

	private void chooseBestSizeSplit(Cell parent) {

		Partition[] bestSplits = null;

		double bestDiff = Double.MAX_VALUE;

		// Try all horizontal splits		
		for (int i = parent.getBottom() + 1; i < parent.getTop(); i++) {			
			Partition[] splits = Common.split(parent, i, true, costEstimator);

			double diff = Math.abs(splits[0].getSizeInBytes() - splits[1].getSizeInBytes());

			if (diff < bestDiff) {
				bestDiff = diff;
				bestSplits = splits;
			}
		}

		// Try all vertical splits
		for (int i = parent.getLeft() + 1; i < parent.getRight(); i++) {
			Partition[] splits = Common.split(parent, i, false, costEstimator);

			double diff = Math.abs(splits[0].getSizeInBytes() - splits[1].getSizeInBytes());

			if (diff < bestDiff) {
				bestDiff = diff;
				bestSplits = splits;
			}	
		}

		if (bestSplits != null) {
			parent.children[0] = new Cell(bestSplits[0].getBottom(), bestSplits[0].getTop(), bestSplits[0].getLeft(), bestSplits[0].getRight());
			parent.children[0].setCost(bestSplits[0].getCost());
			parent.children[0].setSizeInBytes(bestSplits[0].getSizeInBytes());
			parent.children[0].costReductionDueToSplit = bestSplits[0].getSizeInBytes();

			parent.children[1] = new Cell(bestSplits[1].getBottom(), bestSplits[1].getTop(), bestSplits[1].getLeft(), bestSplits[1].getRight());
			parent.children[1].setCost(bestSplits[1].getCost());
			parent.children[1].setSizeInBytes(bestSplits[1].getSizeInBytes());
			parent.children[1].costReductionDueToSplit = bestSplits[1].getSizeInBytes();

			parent.children[0].parent = parent;
			parent.children[1].parent = parent;
		}
	}

	private void chooseBestCostSplit(Cell parent) {

		parent.costReductionDueToSplit = 0;
		Partition[] bestSplits = null;

		double parentCost = costEstimator.getCost(parent);

		// Try all horizontal splits		
		for (int i = parent.getBottom() + 1; i < parent.getTop(); i++) {			
			Partition[] splits = Common.split(parent, i, true, costEstimator);

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
		
		parent.costReductionDueToSplit -= 2 * costEstimator.getSize(parent);

		if (bestSplits != null) {
			parent.children[0] = new Cell(bestSplits[0].getBottom(), bestSplits[0].getTop(), bestSplits[0].getLeft(), bestSplits[0].getRight());
			parent.children[0].setCost(bestSplits[0].getCost());
			parent.children[0].setSizeInBytes(bestSplits[0].getSizeInBytes());
			parent.children[0].parent = parent;

			parent.children[1] = new Cell(bestSplits[1].getBottom(), bestSplits[1].getTop(), bestSplits[1].getLeft(), bestSplits[1].getRight());
			parent.children[1].setCost(bestSplits[1].getCost());
			parent.children[1].setSizeInBytes(bestSplits[1].getSizeInBytes());
			parent.children[1].parent = parent;
		}
	}

	public class Cell extends Partition {
		public Cell(int bottom, int top, int left, int right) {
			super(bottom, top, left, right);
			this.children = new Cell[2];
			this.parent = null;
		}				

		private Cell parent;
		private Cell[] children;
		private double costReductionDueToSplit;
		private double costIncreaseDueToMerge;
	}

	private class MaxHeapComparator implements Comparator<Cell>{
		@Override
		public int compare(Cell x, Cell y) {
			if (x.costReductionDueToSplit < y.costReductionDueToSplit)
				return 1;

			if (x.costReductionDueToSplit > y.costReductionDueToSplit)			
				return -1;

			return 0;
		}
	}

	private class MinHeapComparator implements Comparator<Cell>{
		@Override
		public int compare(Cell x, Cell y) {
			if (x.costIncreaseDueToMerge > y.costIncreaseDueToMerge)
				return 1;

			if (x.costIncreaseDueToMerge < y.costIncreaseDueToMerge)			
				return -1;

			return 0;
		}
	}
}
