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

import kafka.utils._
import kafka.utils.timer._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.metrics.KafkaMetricsGroup

import java.util.LinkedList
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.utils.Utils

import scala.collection._

import com.yammer.metrics.core.Gauge


/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
  * 一个操作,它的处理需要被延迟 最多给定的delayMs.例如delayed produce operation可能正在等待指定数量的确认;
  * a delayed fetch operation  正在等待给定的字节数累积。
  *
  *  完成延迟操作的逻辑在onComplete（）中定义，并且将被调用一次。
  *  一旦操作完成，isCompleted（）将返回true。
  *   onComplete() 可以被forceComplete()触发 ,
  *   可以由forceComplete（）强制触发，如果操作尚未完成，
  *   则强制在delayMs之后调用onComplete（）
  *
  *   或者tryComplete（），它首先检查操作是否可以立即完成，如果是，则调用forceComplete（）。
  *   DelayedOperation的子类需要提供onComplete（）和tryComplete（）的实现。
  *
  *
 */
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {
  //是否完成
  private val completed = new AtomicBoolean(false)

  /**
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
    *
    * 如果现在没玩成的话 强制执行延迟操作.这个函数可以被触发当
    * 1  这个操作已经被验证将在tryComplete（）中可以完成
    * 2  这个操作已经过期了,因此 需要立刻被完成
    *
    * 返回true 如果这个操作被这个调用者完成
    * 注意 并行线程可以尝试完成相同的操作但是仅第一个线程将成功 完成这个操作返回
    * true其它的将返回false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      //取消 超时计时器
      cancel()
      //完成任务
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  def isCompleted(): Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
    * 具体业务逻辑
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   * 尝试完成延迟操作 通过首先检查这个操作是否现在可以被完成.
    * 如果是通过调用forceComplete() 执行完成逻辑,如果forceComplete返回true
    * 返回true.否则返回失败
    *
    *
   */
  def tryComplete(): Boolean

  /*
   * run() method defines a task that is executed on timeout
   * 超时的时候执行
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
  * 一个助手炼狱类，用于簿记的辅助炼狱类延迟了超时操作，并使超时操作到期。
  * 管理DelayedOperation 处理到期的DelayedOperation功能
  *
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  /*
  key表示的是watchers中DelayedOperation关心的对象,value是watchers类型的对象.
  watchers是DelayedOperationPurgatory内部类,表示一个DelayedOperation的集合.底层使用list实现

  TopicPartitionOperationKey 是 delayed-produce关心的对象,表示某个topic的分区
  *
  * 每当leader副本收到follower副本对test-0的 fetchRequest时,都会检查test-0对应的watchers
  * 中的 operation是否满足了执行条件.如果满足执行条件执行条件,就会执行DelayedOperation
  * 向客户端发送response
  *
  * 最终DelayedOperation(DelayedProduce) 会因为到期或满足执行条件而被执行
  *
  * */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out
   * 已经超时的 到期操作的后台线程 */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.size > 0, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    var isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted())
        return false
      watchForOperation(key, operation)

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }

    isCompletedByMe = operation synchronized operation.tryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (! operation.isCompleted()) {
      timeoutTimer.add(operation)
      if (operation.isCompleted()) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched() = allWatchers.map(_.watched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def delayed() = timeoutTimer.size

  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.watched == 0) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {

    private[this] val operations = new LinkedList[T]()

    def watched: Int = operations synchronized operations.size

    // add the element to watch
    def watch(t: T) {
      operations synchronized operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    // 遍历这个list 尝试完成一些 观察到的事件
    def tryCompleteWatched(): Int = {

      var completed = 0
      operations synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next()
          if (curr.isCompleted) {
            // another thread has completed this operation, just remove it
            //其它线程已经完成了这个操作,移除
            iter.remove()
            //如果尝试完成成功
          } else if (curr synchronized curr.tryComplete()) {
            completed += 1
            //移除
            iter.remove()
          }
        }
      }
       //如果队列的长度为0,移除这个操作
      if (operations.size == 0)
        removeKeyIfEmpty(key, this)

      completed
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0
      operations synchronized {
        val iter = operations.iterator()
        while (iter.hasNext) {
          val curr = iter.next()
          if (curr.isCompleted) {
            iter.remove()
            purged += 1
          }
        }
      }

      if (operations.size == 0)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long) {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    //触发一个清理,如果完成的但仍然被观察的操作大于清洗threshold
    //该数字是通过估计的总操作数和未决的延迟操作数的差来计算的。
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
    * 1 推动时间轮
    * 2 清理完成的delay operation
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    override def doWork() {
      advanceClock(200L)
    }
  }
}
