package core;

import index.RTree;

import java.util.List;


public class StaticPartitioning {

	private CostEstimator costEstimator;
	
	public RTree<Partition> partitionsRTree;

	// Just return cost
	public double processNewQuery(Partition newQuery) {
		double cost = 0;
		List<Partition> overlappingPartitions = partitionsRTree.searchExclusive(newQuery.getCoords(), newQuery.getDimensions());
		
		for (Partition p : overlappingPartitions) {
			cost += costEstimator.getSize(p);			
		}

		return cost;
	}

	public StaticPartitioning(CostEstimator costEstimator, int k) {
		
		this.costEstimator = costEstimator;
		
		DynamicPartitioning dynamic = new DynamicPartitioning(costEstimator, k, 1);
		partitionsRTree = new RTree<Partition>(10, 5, 2);
		for (Partition p : dynamic.initialPartitions()) {
			partitionsRTree.insert(p.getCoords(), p.getDimensions(), p);
		}
	}

}
