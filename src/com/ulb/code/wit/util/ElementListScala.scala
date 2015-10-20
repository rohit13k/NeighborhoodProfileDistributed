package com.ulb.code.wit.util
import scala.util.control.Breaks._
/**
 * @author Rohit
 */
class ElementListScala extends Serializable {
  def apply() {

  }
  var elemList: Array[(Int, Long)] = null
  def getElement(index: Int): (Int, Long) = {
    if (elemList != null)
      elemList(index)
    else
      null
  }
  def getTop(): (Int, Long) = {
    getElement(0)

  }
  def length(): Int = this.elemList.length
  def getTopElementValue(): Int = {
    if (elemList != null) {
      getTop()._1
    } else {
      -1
    }
  }
  def getElementValue(window: Long): Int = {
    for (i <- 0 until this.elemList.length - 1) {
      if (this.elemList(i)._2 >= window) {
        return this.elemList(i)._1
      }
    }
    -1
  }
  def addNewElement(value: Int, timestamp: Long): Boolean = {
    var addedNew = false;
    var changed = true;
    if (this.elemList == null) {
      this.elemList = Array((value, timestamp))

      addedNew = true
    } else {
      // need to complete
      // ArrayList<Element> newList = new ArrayList<Element>();

      for (i <- 0 until this.elemList.length - 1) {

        var oldElement = this.elemList(i);
        if (oldElement._1 > (value)) {
          // newList.add(oldElement);
        } else if (oldElement._1 == value) {
          if (oldElement._2 >= timestamp) {
            // newList.add(oldElement);
            changed = false;
          } else {
            // newList.add(newElement);
            this.elemList.slice(i, this.elemList.length)
            this.elemList ++ Array(value, timestamp)

          }
          addedNew = true
          break
        } else {
          // newList.add(newElement);
          this.elemList.slice(i, this.elemList.length)
          this.elemList ++ Array(value, timestamp)
          addedNew = true

          break
        }

      }
    }
    if (!addedNew) {
      if (this.elemList(this.elemList.length - 1)._2 < timestamp)
        this.elemList ++ Array(value, timestamp)
      else {
        changed = false
      }
    }

    changed

  }
}