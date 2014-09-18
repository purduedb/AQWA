package experiments;

import java.util.ArrayList;



import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

import drawing.Line;


import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;

import java.util.LinkedList;

import javax.swing.JComponent;
import javax.swing.JFrame;

public class Demo extends JComponent {

	private static int width = 1000;
	private static int height = 1000;
	
	private static int sleepInMilliSec = 10;
	
	private static int k = 100;

	private static  LinkedList<Line> lines = new LinkedList<Line>();
	private static String partitions;
	private static DynamicPartitioning dynamic;

	private static ArrayList<Partition> getRandomQLoad() {
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int i = 0; i < 100000; i++) {
			int left = (int)(Math.random() * 100);
			int right = (int)(Math.random() * 100) + left;
			int bottom = (int)(Math.random() * 100);
			int top = (int)(Math.random() * 100) + bottom;

			//qLoad.add(new Partition(bottom, top, left, right));

			left = 800 + (int)(Math.random() * 100);
			right = (int)(Math.random() * 100) + left;
			bottom = 800 + (int)(Math.random() * 100);
			top = (int)(Math.random() * 100) + bottom;

			qLoad.add(new Partition(bottom, top, left, right));

			left = 300 + (int)(Math.random() * 100);
			right = (int)(Math.random() * 100) + left;
			bottom = 500 + (int)(Math.random() * 100);
			top = (int)(Math.random() * 100) + bottom;

			qLoad.add(new Partition(bottom, top, left, right));
		}
		return qLoad;
	}

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		for (Line line : lines) {
			g.setColor(line.color);
			g.drawLine(line.x1, line.y1, line.x2, line.y2);
		}
	}

	private static void showLines(JComponent component) {
		String[] partitionsStrArr = partitions.split(";");
		for (String partitionStr : partitionsStrArr) {
			String[] coords = partitionStr.split(",");

			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), getColor());
			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), getColor());
			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), getColor());
			addLine(Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), getColor());
		}
		component.repaint();
	}

	private static Color getColor() {
		return Color.black;
	}

	private static void addLine(int x1, int x2, int x3, int x4, Color color) {
		double scale = 0.85;
		int delta = 20;
		x1 = delta + (int) (scale * x1);
		x2 = delta + (int) (scale * x2);
		x3 = delta +(int) (scale * x3);
		x4 = delta + (int) (scale * x4);

		lines.add(new Line(x1, x2, x3, x4, color));
	}

	public static void main(String[] args) throws InterruptedException {
		JFrame testFrame = new JFrame();
		testFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		final Demo comp = new Demo();
		comp.setPreferredSize(new Dimension(900, 900));
		testFrame.getContentPane().add(comp, BorderLayout.CENTER);

		CostEstimator costEstimator = new CostEstimator(null, null, width, height);
		Solution solution = new Solution();
		dynamic = new DynamicPartitioning(costEstimator, k);
		for (Partition p : dynamic.initialPartitions())
			solution.addPartition(p);

		System.out.println(solution.getPartitions().size());

		partitions = solution.toString();
		System.out.println(partitions);
		showLines(comp);
		testFrame.pack();
		testFrame.setVisible(true);

		int i = 0;
		for (Partition p : getRandomQLoad()) {
			dynamic.processNewQuery(p);

			if (i++ % 55 ==0) {
				Thread.sleep(sleepInMilliSec);

				lines.clear();
				addLine(p.getLeft(), p.getBottom(), p.getRight(), p.getBottom(), Color.RED);
				addLine(p.getLeft(), p.getTop(), p.getRight(), p.getTop(), Color.RED);
				addLine(p.getLeft(), p.getBottom(), p.getLeft(), p.getTop(), Color.RED);
				addLine(p.getRight(), p.getBottom(), p.getRight(), p.getTop(), Color.RED);
				solution = new Solution();
				for (Partition part : dynamic.currentPartitions)
					solution.addPartition(part);

				partitions = solution.toString();
				//System.out.println(partitions);
				showLines(comp);
				testFrame.pack();
				testFrame.setVisible(true);
			}
		}

		testFrame.pack();
		testFrame.setVisible(true);
	}

}
