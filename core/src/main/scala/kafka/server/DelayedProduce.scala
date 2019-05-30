/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server


import java.util.concurrent.TimeUnit

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, //produceRequest中追加到此分区的最后
                                 //一个消息的offset
                                  responseStatus: PartitionResponse) {
  //是否完成了requiredOffset之前的消息同步
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.errorCode, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short, //acks
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
  * 一个延迟操作,它可以被replica manager 创建, in the produce operation purgatory被观察
 */
class DelayedProduce(delayMs: Long,  //delayed produce的延时时长
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     //满足条件或到期执行,DelayedProduce 中的回调函数.其主要功能是向RequestChannels
                     //responseQueue添加ProduceResponse,之后processor线程会将其发送给客户端
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code
  //更新acks pending variable 首先根据error code
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.errorCode == Errors.NONE.code) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      //当需要的acks到达后,会清除超时状态
      status.responseStatus.errorCode = Errors.REQUEST_TIMED_OUT.code
    } else {
      //如果追加日志已经抛出了异常,则不必等待此partition对应的isr返回ack了
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
    *
    * 这个delayed 生产操作 可能被完成 如果它生产到的每个分区满足下面的一个
    * case a 这个broker不在是leader 在response设置一个错误
    * case b 这个broker是一个leader
    * b1 如果有一个本地的异常被抛出 当检查是否requiredAcks 副本已经被捕获:在response
    * 设置一个错误
    * b2 否则,设置没有错误的response
   */
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    //检查每个分区 是否仍然有一个pending acks
    produceMetadata.produceStatus.foreach { case (topicAndPartition, status) =>
      trace("Checking produce satisfaction for %s, current status %s"
        .format(topicAndPartition, status))
      // skip those partitions that have already been satisfied
      //跳过已经被满足的分区
      if (status.acksPending) {
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val (hasEnough, errorCode) = partitionOpt match {
          case Some(partition) =>
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
        }
        if (errorCode != Errors.NONE.code) {
          // Case B.1
          status.acksPending = false
          status.responseStatus.errorCode = errorCode
        } else if (hasEnough) {
          // Case B.2
          status.acksPending = false
          status.responseStatus.errorCode = Errors.NONE.code
        }
      }
    }

    // check if each partition has satisfied at lease one of case A and case B
    if (! produceMetadata.produceStatus.values.exists(p => p.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
    * 完成后，返回当前响应状态以及每个分区的错误代码
   */
  override def onComplete() {
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

