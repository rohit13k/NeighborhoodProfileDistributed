package com.ulb.code.wit.main
import org.apache.spark.internal.Logging
import scala.reflect.ClassTag
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
object NewPregel {

  def apply[VD: ClassTag, ED: ClassTag, A: ClassTag](graph: Graph[VD, ED],
                                                     initialMsg: A,
                                                     maxIterations: Int = Int.MaxValue,
                                                     activeDirection: EdgeDirection = EdgeDirection.Either)(vprog: (VertexId, VD, A) => VD,
                                                                                                            mapFunc: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
                                                                                                            mergeMsg: (A, A) => A): Graph[VD, ED] =
    {
      def sendMsg(ctx: EdgeContext[VD, ED, A]) {
        mapFunc(ctx.toEdgeTriplet).foreach { kv =>
          val id = kv._1
          val msg = kv._2
          if (id == ctx.srcId) {
            ctx.sendToSrc(msg)
          } else {
            assert(id == ctx.dstId)
            ctx.sendToDst(msg)
          }
        }
      }
      require(maxIterations > 0, s"Maximum number of iterations must be greater than 0," +
        s" but got ${maxIterations}")

      var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
      // compute the messages
      //   g.vertices.count()
      var messages = g.aggregateMessages(sendMsg, mergeMsg)
      var activeMessages = messages.count()
      // Loop
      var prevG: Graph[VD, ED] = null
      var i = 0
      while (activeMessages > 0 && i < maxIterations) {
        // Receive the messages and update the vertices.
        //        println("Pregel started iteration " + i)
        prevG = g
        //new line added to force the replication of triplets with new vertex attribute
          g = Graph(g.vertices, g.edges)

        g = g.joinVertices(messages)(vprog).cache()

        val oldMessages = messages
        // Send new messages, skipping edges where neither side received a message. We must cache
        // messages so it can be materialized on the next line, allowing us to uncache the previous
        // iteration.
        
        messages = g.aggregateMessages(sendMsg, mergeMsg, TripletFields.All).cache()
        // The call to count() materializes `messages` and the vertices of `g`. This hides oldMessages
        // (depended on by the vertices of g) and the vertices of prevG (depended on by oldMessages
        // and the vertices of g)
        //        println("aggregate msg done");
        
        activeMessages = messages.count()
         
        //        println("PregeCl finished iteration " + i)

        // Unpersist the RDDs hidden by newly-materialized RDDs
        oldMessages.unpersist(blocking = false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
        // count the iteration
        i += 1
      }
      messages.unpersist(blocking = false)
      g
    } // end of apply

}