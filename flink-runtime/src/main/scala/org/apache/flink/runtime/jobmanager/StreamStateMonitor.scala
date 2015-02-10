package org.apache.flink.runtime.jobmanager

import akka.actor._
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph, ExecutionJobVertex, ExecutionVertex}
import org.apache.flink.runtime.jobgraph.JobVertexID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, _}


object StreamStateMonitor {

  def props(context: ActorContext, executionGraph: ExecutionGraph, interval: FiniteDuration = 10 seconds): ActorRef = {

    val vertices: Iterable[ExecutionVertex] = getExecutionVertices(executionGraph)
    val monitor = context.system.actorOf(Props(new StreamStateMonitor(executionGraph,
      vertices, vertices.map(x => (x.getJobVertex.getJobVertexId, 0L)).toMap, interval, 0L)))
    monitor ! InitBarrierScheduler
    monitor
  }

  private def getExecutionVertices(executionGraph: ExecutionGraph): Iterable[ExecutionVertex] = {
    for (
      execJobVertex: ExecutionJobVertex <- executionGraph.getAllVertices.values();
      execVertex: ExecutionVertex <- execJobVertex.getTaskVertices)
    yield execVertex
  }
}

class StreamStateMonitor(executionGraph: ExecutionGraph, vertices: Iterable[ExecutionVertex],
                         acks: Map[JobVertexID, Long], interval: FiniteDuration, curId: Long)
        extends Actor with ActorLogMessages with ActorLogging {

  override def receiveWithLogMessages: Receive = {
    case InitBarrierScheduler =>
      context.system.scheduler.schedule(interval, interval, self, BarrierTimeout)
      log.info("[FT-MONITOR] Started Stream State Monitor for job {}{}",
        executionGraph.getJobID, executionGraph.getJobName)
    case BarrierTimeout =>
      curId += 1
      log.info("[FT-MONITOR] Sending Barrier to vertices of Job " + executionGraph.getJobName)
      //TODO we only send barriers to input job vertices, should check the case of iteration heads
      vertices.filter(v => v.getJobVertex.getJobVertex.isInputVertex).foreach(vertex 
      => vertex.getCurrentAssignedResource.getInstance.getTaskManager
              ! BarrierReq(vertex.getCurrentExecutionAttempt.getAttemptId, curId))
    case BarrierAck(jobVertexID, checkpointID) =>
      acks += (jobVertexID -> checkpointID) 
      log.info("[FT-MONITOR] Updated acknowledgement table")
      log.info(acks.toString)
  }
}

case class BarrierTimeout()

case class InitBarrierScheduler()

case class BarrierReq(attemptID: ExecutionAttemptID, checkpointID: Long)

case class BarrierAck(jobVertexID: JobVertexID, checkpointID: Long)



