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
package kafka.utils.timer

import java.util.concurrent.{TimeUnit, Delayed}
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import kafka.utils.{SystemTime, threadsafe}

import scala.math._

@threadsafe
//环形双向链表
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  //TimerTaskList使用 哨兵 形成双向链接循环列表
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root
  //记录整个 TaskList的超时时间,  与SystemTime.milliseconds比较
  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  //设置桶的过期时间,返回true 如果过期时间改变了
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  //得到过期时间
  def getExpiration(): Long = {
    expiration.get()
  }

  // Apply the supplied function to each of tasks in this list
  //将提供的函数应用于此列表中的每个任务
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  //添加一个 timer task到这个List
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      //删除timer task entry（如果它已在任何其他列表中）
      // 我们在下面的同步块之外执行此操作以避免死锁。
      // 我们可能会重试，直到timerTaskEntry.list变为null。
      timerTaskEntry.remove()

      synchronized {
        timerTaskEntry.synchronized {
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            //把这个 timer task entry放到 list的结尾
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  //从这个list中移除给定的timer task entry
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  //移除所有的 task entries ,应用提供的函数到它们中的每一个
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      var head = root.next
      while (head ne root) {
        remove(head)
        f(head)
        head = root.next
      }
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - SystemTime.milliseconds, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {

    val other = d.asInstanceOf[TimerTaskList]

    if(getExpiration < other.getExpiration) -1
    else if(getExpiration > other.getExpiration) 1
    else 0
  }

}
//TimerTask字段指向了对应的TimerTask任务
private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {

  @volatile
  var list: TimerTaskList = null
  var next: TimerTaskEntry = null
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  // 如果timerTask 已经被an existing timer task entry持有
  // setTimerTaskEntry将移除它
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }

  def remove(): Unit = {
    //找到对应的list 移除
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    //当一个线程正在移动这个entry从一个task entry list到另一个
    //由于list值的改变  这可能失败(移除这个entry) ,所以我们将重试直到list变成null
    //在极少数情况下，此线程看到null并退出循环，但另一个线程稍后将该条目插入另一个列表。
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    this.expirationMs compare that.expirationMs
  }
}

