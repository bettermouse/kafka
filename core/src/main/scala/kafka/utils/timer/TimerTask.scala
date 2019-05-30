/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.utils.timer

//链表中的每一项表示的都是定时任务项TimerTaskEntry其中封装了真正的定时任务TimerTask。
trait TimerTask extends Runnable {
  //记录任务的延迟时间
  val delayMs: Long // timestamp in millisecond

  private[this] var timerTaskEntry: TimerTaskEntry = null

  def cancel(): Unit = {
    synchronized {
      //cancel的时候要从把timerTaskEntry从 list里面移除
      if (timerTaskEntry != null) timerTaskEntry.remove()
      timerTaskEntry = null
    }
  }

  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      //如果timerTask已经被一个已经存在的timer task持有,我们将首先移除这个实体
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      timerTaskEntry = entry
    }
  }

  private[timer] def getTimerTaskEntry(): TimerTaskEntry =
  //!!! 这个synchronized是我自己加的.不然感觉有问题
    synchronized {
      timerTaskEntry
    }

}
