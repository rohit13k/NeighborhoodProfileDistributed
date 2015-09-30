package com.ulb.code.wit.util;

import java.io.Serializable;
import java.util.ArrayList;

public class ElementList<T> extends ArrayList<Element> implements Serializable {

	public Element getTopElement() {
		return this.get(0);
	}

	public int getTopElementValue() {
		return this.get(0).getValue();
	}

	public int getElementValue(long window) {
		for (int i = 0; i < this.size(); i++) {
			if (this.get(i).getTimestamp() >= window) {
				return this.get(i).getValue();
			}
		}
		return -1;
	}

	public Element getElement(int index) {
		return this.get(index);
	}

	public boolean addNewElement(int value, long timestamp) {
		// Element newElement = new Element(value, timestamp);

		boolean addedNew = false;
		boolean changed = true;
		if (this.size() == 0) {
			this.add(new Element(value, timestamp));
			addedNew = true;
		} else {
			// need to complete
			// ArrayList<Element> newList = new ArrayList<Element>();
			for (int i = 0; i < this.size(); i++) {
				Element oldElement = this.get(i);
				if (oldElement.getValue() > (value)) {
					// newList.add(oldElement);
				} else if (oldElement.getValue() == value) {
					if (oldElement.getTimestamp() >= timestamp) {
						// newList.add(oldElement);
						changed = false;
					} else {
						// newList.add(newElement);
						this.removeRange(i, this.size());
						this.add(new Element(value, timestamp));
					}
					addedNew = true;
					break;
				} else {
					// newList.add(newElement);
					this.removeRange(i, this.size());
					this.add(new Element(value, timestamp));
					addedNew = true;
					break;
				}

			}
			if (!addedNew) {
				if (this.get(this.size() - 1).getTimestamp() < timestamp)
					this.add(new Element(value, timestamp));
				else {
					changed = false;
				}
			}

		}
		return changed;
	}

	public boolean addNewElement(Element newEle) {
		// Element newElement = new Element(value, timestamp);

		return addNewElement(newEle.getValue(), newEle.getTimestamp());
	}
}
