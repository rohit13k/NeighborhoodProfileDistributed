package com.ulb.code.wit.util;

import java.io.Serializable;

public class Element implements Serializable{
	private int value;
	private long timestamp;

	public Element(int value,long time){
		this.value=value;
		this.timestamp=time;
	}
	
	public String toString(){
		return value+"@"+timestamp;
	}
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Element))
			return false;
		if (obj == this)
			return true;
		else {
			Element ne = (Element) obj;
			if (ne.value == this.value) {
				return true;
			} else {
				return false;
			}
		}

	}
	public boolean isBigger(Element ne){
		if(this.value>ne.value){
			return true;
		}
		return false;
	}
	public boolean isLater(Element ne){
		if(this.timestamp>ne.timestamp){
			return true;
		}
		return false;
	}
	public int getValue(){
		return this.value;
	}
	public long getTimestamp(){
		return this.timestamp;
	}
}
