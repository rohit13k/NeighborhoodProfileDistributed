package com.ulb.code.wit.util;

import java.io.Serializable;

import com.ulb.code.wit.main.SlidingHLL;

public class PropogationObjectApprox implements Serializable {

	private Long targetNode;

	private SlidingHLL sourceSkecth;
	private Long sourceNodeName;
	private long timestamp;
	private int distance;

	public PropogationObjectApprox(Long targetNode, Long sourceNode,
			SlidingHLL sourceSkecth, long timestamp, int distance) {
		this.targetNode = targetNode;
		this.sourceNodeName = sourceNode;
		this.sourceSkecth = sourceSkecth;
		this.timestamp = timestamp;
		this.distance = distance;

	}

	public Long getTargetNode() {
		return targetNode;
	}

	public Long getSourceNode() {
		return sourceNodeName;
	}

	public void setTargetNode(Long targetNode) {
		this.targetNode = targetNode;
	}

	public SlidingHLL getSourceElement() {
		return sourceSkecth;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getDistance() {
		return distance;
	}

	public String toString() {
		return "src : " + this.sourceNodeName + " target : " + this.targetNode
				+ " distance : " + this.distance + " time : " + this.timestamp;
	}

	public boolean equals(Object obj) {
		if (!(obj instanceof PropogationObjectApprox))
			return false;
		if (obj == this)
			return true;
		else {
			PropogationObjectApprox pe = (PropogationObjectApprox) obj;
			if (pe.targetNode == (targetNode) && pe.distance == this.distance
					&& pe.sourceNodeName == (this.sourceNodeName)
					&& pe.timestamp == this.timestamp) {
				return true;
			} else {
				return false;
			}

		}
	}

}
