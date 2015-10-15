package com.ulb.code.wit.util
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
/**
 * @author Rohit
 */
object PregelCorrected {
  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag](graph: Graph[VD, ED],
                                                     initialMsg: A,
                                                     maxIterations: Int = Int.MaxValue,
                                                     activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VD, A) => VD,
                                                                                                            sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                                                            mergeMsg: (A, A) => A): Graph[VD, ED] =
    {
      var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()

      // compute the messages
      var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
      var activeMessages = messages.count()

      // Loop
      var prevG: Graph[VD, ED] = null
      var i = 0
      while (activeMessages > 0 && i < maxIterations) {
        // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
        val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
        // Update the graph with the new vertices.
        prevG = g
        g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
        g.cache()

        val oldMessages = messages
        // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
        // get to send messages. We must cache messages so it can be materialized on the next line,
        // allowing us to uncache the previous iteration.
        val Rvel = g.vertices
        val Redge = g.edges
        g = Graph(Rvel, Redge)
        //        Rtrip = g.triplets.collect()
        messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
        // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
        // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
        // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
        activeMessages = messages.count()

        println("Pregel finished iteration " + i)

        // Unpersist the RDDs hidden by newly-materialized RDDs
        oldMessages.unpersist(blocking = false)
        newVerts.unpersist(blocking = false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
        // count the iteration
        i += 1
      }

      g
    }
}