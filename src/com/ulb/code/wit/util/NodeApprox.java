package com.ulb.code.wit.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ulb.code.wit.main.SlidingHLL;
import com.ulb.code.wit.util.*;

public class NodeApprox implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7452945222716840101L;
	private SlidingHLL nodeSummary;
	private ArrayList<SlidingHLL> distanceWiseSummaries;
	private int currentSuperStep = 0;
	private int numberOfBucket = 256;
	private boolean ischanged = false;

	public NodeApprox(int distance, int numberOfBucket) {

		this.numberOfBucket = numberOfBucket;
		this.nodeSummary = new SlidingHLL(numberOfBucket);
		this.distanceWiseSummaries = new ArrayList<SlidingHLL>();

		for (int i = 0; i < distance; i++) {
			this.distanceWiseSummaries.add(new SlidingHLL(numberOfBucket));
		}

	}

	public SlidingHLL getNodeSummary() {
		nodeSummary = new SlidingHLL(numberOfBucket);
		SlidingHLL oldSummary;
		int bucketNo = 0;

		Element elem;
		for (int i = 0; i < distanceWiseSummaries.size(); i++) {
			oldSummary = distanceWiseSummaries.get(i);
			bucketNo = 0;
			for (ElementList el : oldSummary.getBuckets()) {
				if (null != el) {
					for (int j = 0; j < el.size(); j++) {
						elem = el.getElement(j);
						if (elem != null)
							nodeSummary.merge(bucketNo, elem);
					}

				}

				bucketNo++;
			}
		}

		return nodeSummary;
	}

	public ArrayList<SlidingHLL> getDistanceWiseSummaries() {
		return distanceWiseSummaries;
	}

	public void setDistanceWiseSummaries(
			ArrayList<SlidingHLL> distanceWiseSummaries) {
		this.distanceWiseSummaries = distanceWiseSummaries;
	}

	public int getCurrentSuperStep() {
		return currentSuperStep;
	}

	public void setCurrentSuperStep(int currentSuperStep) {
		this.currentSuperStep = currentSuperStep;
	}

	public boolean isIschanged() {
		return ischanged;
	}

	public void setIschanged(boolean ischanged) {
		this.ischanged = ischanged;
	}

}
