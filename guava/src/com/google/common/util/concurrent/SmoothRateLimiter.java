/*
 * Copyright (C) 2012 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.util.concurrent;

import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.annotations.GwtIncompatible;
import com.google.common.math.LongMath;
import java.util.concurrent.TimeUnit;


/**
 * 平滑限流实现器，也是一个抽象类。
 */
@GwtIncompatible
abstract class SmoothRateLimiter extends RateLimiter {

  /**
   * This implements the following function where coldInterval = coldFactor * stableInterval.
   *
   * <pre>
   *          ^ throttling
   *          |
   *    cold  +                  /
   * interval |                 /.
   *          |                / .
   *          |               /  .   ← "warmup period" is the area of the trapezoid between
   *          |              /   .     thresholdPermits and maxPermits
   *          |             /    .
   *          |            /     .
   *          |           /      .
   *   stable +----------/  WARM .
   * interval |          .   UP  .
   *          |   stable . PERIOD.
   *          |          .       .
   *        0 +----------+-------+--------------→ storedPermits
   *          0 thresholdPermits maxPermits
   * </pre>
   *
   *
   * 1、图中有两个阴影面积，一个用 stable，另外一个warm up period。在预热算法中，这两个阴影面积的关系与冷却因子相关。
   * 2、冷却因子 coldFactor 表示的含义为 coldInterval 与 stableInterval 的比值。
   * 3、warm up period 阴影面积 与 stable 阴影面积的比值等于 (coldInterval - stableInterval ) / stableInterval ，
   * 例如 SmoothWarmingUp 固定的冷却因子为3，那么 coldInterval 与 stableInterval 的比值为 3，
   * 那 (coldInterval - stableInterval ) / stableInterval 则为 2。
   * 4、在预热算法中与数学中的积分相关（笔者对这方面的数学知识一窍不通），故这里只展示结论，而不做推导，
   * 若阴影 WARM UP PERIOD 的面积等于 warmupPeriod,那阴影stable的面积等于 warmupPeriod/2。
   * 5、存在如下等式 warmupPeriod/2 = thresholdPermits * stableIntervalMicros (长方形的面积)
   *   同样存在如下等式 warmupPeriod = 0.5 * (stableInterval + coldInterval) * (maxPermits - thresholdPermits) （梯形面积，(上底 + 下底 * 高 / 2) ）
   */


  /**
   * 自带预热机制的限流器实现类型。
   */
  static final class SmoothWarmingUp extends SmoothRateLimiter {
    private final long warmupPeriodMicros;
    /**
     * The slope of the line from the stable interval (when permits == 0), to the cold interval
     * (when permits == maxPermits)
     */
    private double slope;
    private double thresholdPermits;
    private double coldFactor;

    SmoothWarmingUp(
        SleepingStopwatch stopwatch, long warmupPeriod, TimeUnit timeUnit, double coldFactor) {
      super(stopwatch);
      this.warmupPeriodMicros = timeUnit.toMicros(warmupPeriod);
      this.coldFactor = coldFactor;
    }

    @Override
    void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
      /**
       *  double permitsPerSecond, 每秒的许可数
       *  double stableIntervalMicros：稳定的获取一个许可的时间
       */
      double oldMaxPermits = maxPermits;
      //根据冷却因子来计算冷却间隔  冷却因子 coldFactor 为 冷却间隔与稳定间隔的比例
      double coldIntervalMicros = stableIntervalMicros * coldFactor;
      //通过 warmupPeriod/2 = thresholdPermits * stableIntervalMicros 等式，求出 thresholdPermits 的值。
      thresholdPermits = 0.5 * warmupPeriodMicros / stableIntervalMicros;
      //根据 warmupPeriod = 0.5 * (stableInterval + coldInterval) * (maxPermits - thresholdPermits) 表示可求出 maxPermits 的数量。
      maxPermits =
          thresholdPermits + 2.0 * warmupPeriodMicros / (stableIntervalMicros + coldIntervalMicros);
      //斜率，表示的是从 stableIntervalMicros 到 coldIntervalMicros 这段时间，许可数量从 thresholdPermits 变为 maxPermits 的增长速率。
      slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits - thresholdPermits);

      //根据 maxPermits 更新当前存储的许可，即当前剩余可消耗的许可数量。
      if (oldMaxPermits == Double.POSITIVE_INFINITY) {
        // if we don't special-case this, we would get storedPermits == NaN, below
        storedPermits = 0.0;
      } else {
        storedPermits =
            (oldMaxPermits == 0.0)
                ? maxPermits // initial state is cold
                : storedPermits * maxPermits / oldMaxPermits;
      }
    }

    @Override
    long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
      /**
       * double storedPermits：当前存储的许可数量。
       * double permitsToTake：本次申请需要的许可数量。
       */

      /**
       * availablePermitsAboveThreshold ，当前超出 thresholdPermits 的许可个数，
       * 如果超过 thresholdPermits ，申请许可将来源于超过的部分，只有其不足后，才会从 thresholdPermits 中申请
       */
      double availablePermitsAboveThreshold = storedPermits - thresholdPermits;
      long micros = 0;
      // measuring the integral on the right part of the function (the climbing line)
      /**
       * 如果当前存储的许可数量超过了稳定许可 thresholdPermits  即存在预热的许可数量的申请逻辑
       */
      if (availablePermitsAboveThreshold > 0.0) {
        //取超出的部分和本次申请需要的许可数量较小的那个
        double permitsAboveThresholdToTake = min(availablePermitsAboveThreshold, permitsToTake);
        // TODO(cpovirk): Figure out a good name for this variable.
        //
        double length = permitsToTime(availablePermitsAboveThreshold)
                + permitsToTime(availablePermitsAboveThreshold - permitsAboveThresholdToTake);

        micros = (long) (permitsAboveThresholdToTake * length / 2.0);
        permitsToTake -= permitsAboveThresholdToTake;
      }
      // measuring the integral on the left part of the function (the horizontal line)
      /**
       * ：从稳定区间获取一个许可的时间，就容易理解，为固定的 stableIntervalMicros 。
       */
      micros += (stableIntervalMicros * permitsToTake);
      return micros;
    }

    private double permitsToTime(double permits) {
      /**
       *  slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits - thresholdPermits);
       *
       *  permits = (storedPermits - thresholdPermits;)
       */
      return stableIntervalMicros + permits * slope;
    }

    @Override
    double coolDownIntervalMicros() {
      //用生成这些许可的总时间除以现在已经生成的许可数，即可得到当前时间点平均一个许可的生成时间
      return warmupPeriodMicros / maxPermits;
    }
  }

  /**
   * This implements a "bursty" RateLimiter, where storedPermits are translated to zero throttling.
   * The maximum number of permits that can be saved (when the RateLimiter is unused) is defined in
   * terms of time, in this sense: if a RateLimiter is 2qps, and this time is specified as 10
   * seconds, we can save up to 2 * 10 = 20 permits.
   */

  /**
   * 适应于突发流量的限流器。
   */
  static final class SmoothBursty extends SmoothRateLimiter {
    /** The work (permits) of how many seconds can be saved up if this RateLimiter is unused? */
    /**
     * 为允许的突发流量的时间，这里默认为 1.0，表示一秒，会影响最大可存储的许可数。
     */
    final double maxBurstSeconds;

    SmoothBursty(SleepingStopwatch stopwatch, double maxBurstSeconds) {
      super(stopwatch);
      /**
       * maxBurstSeconds 为允许的突发流量的时间，这里默认为 1.0，表示一秒，会影响最大可存储的许可数。
       */
      this.maxBurstSeconds = maxBurstSeconds;
    }

    @Override
    void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
      /**
       *  double permitsPerSecond, 每秒的许可数
       *  double stableIntervalMicros：稳定的获取一个许可的时间
       */

      // 初始化storedPermits 的值，该限速器支持在运行过程中动态改变 permitsPerSecond 的值。
      double oldMaxPermits = this.maxPermits;
      //最大许可 = 允许的突发流量的时间 + 每秒的许可数
      maxPermits = maxBurstSeconds * permitsPerSecond;
      if (oldMaxPermits == Double.POSITIVE_INFINITY) {
        // if we don't special-case this, we would get storedPermits == NaN, below
        storedPermits = maxPermits;
      } else {
        storedPermits =
            (oldMaxPermits == 0.0)
                ? 0.0 // initial state
                : storedPermits * maxPermits / oldMaxPermits;
      }
    }

    @Override
    long storedPermitsToWaitTime(double storedPermits, double permitsToTake) {
      return 0L;
    }

    @Override
    double coolDownIntervalMicros() {
      return stableIntervalMicros;
    }
  }

  /**
   * The currently stored permits.
   *  当前可用的许可数量
   */
  double storedPermits;

  /**
   * The maximum number of stored permits.
   */
  double maxPermits;

  /**
   * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
   * per second has a stable interval of 200ms.
   */
  double stableIntervalMicros;

  /**
   * The time when the next request (no matter its size) will be granted. After granting a request,
   * this is pushed further in the future. Large requests push this further than small requests.
   * 下一次可以免费获取许可的时间
   */
  private long nextFreeTicketMicros = 0L; // could be either in the past or future

  private SmoothRateLimiter(SleepingStopwatch stopwatch) {
    super(stopwatch);
  }

  @Override
  final void doSetRate(double permitsPerSecond, long nowMicros) {
    /**
     * double permitsPerSecond：每秒的许可数，即TPS。
     * long nowMicros：统已运行时间。
     */

    /**
     * 基于当前时间重置 SmoothRateLimiter 内部的 storedPermits(已存储的许可数量) 与 nextFreeTicketMicros(下一次可以免费获取许可的时间) 值，
     * 所谓的免费指的是无需等待就可以获取设定速率的许可，
     *    注意 SmoothBursty和SmoothWarmingUp的逻辑不完全一样
     *
     */
    resync(nowMicros);
    /**
     * 根据 TPS 算出一个稳定的获取1个许可的时间。以一秒发放5个许可，即限速为5TPS，那发放一个许可的时间间隔为 200ms，
     * stableIntervalMicros 变量是以微妙为单位。
     */
    double stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
    this.stableIntervalMicros = stableIntervalMicros;
    //SmoothBursty.doSetRate
    //SmoothWarmingUp.doSetRate
    doSetRate(permitsPerSecond, stableIntervalMicros);
  }

  abstract void doSetRate(double permitsPerSecond, double stableIntervalMicros);

  @Override
  final double doGetRate() {
    return SECONDS.toMicros(1L) / stableIntervalMicros;
  }

  @Override
  final long queryEarliestAvailable(long nowMicros) {
    return nextFreeTicketMicros;
  }

  @Override
  final long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
    //在尝试申请许可之前，先根据当前时间即发放许可速率更新 storedPermits 与 nextFreeTicketMicros（下一次可以免费获取许可的时间）。
    resync(nowMicros);
    //下一次可以免费获取许可的时间
    long returnValue = nextFreeTicketMicros;
    //计算本次能从 storedPermits 中消耗的许可数量，取需要申请的许可数量与当前可用的许可数量（storedPermits）的最小值，用 storedPermitsToSpend 表示。
    double storedPermitsToSpend = min(requiredPermits, this.storedPermits);
    //如果需要申请的许可数量( requiredPermits )大于当前剩余许可数量( storedPermits )，则还需要等待新的许可生成，
    // 如果requiredPermits < storedPermits  则freshPermits = 0
    // 如果 requiredPermits >  storedPermits   则freshPermits > 0 表示本次申请需要阻塞一定时间。
    double freshPermits = requiredPermits - storedPermitsToSpend;
    /**
     * 计算本次申请需要等待的时间，storedPermitsToWaitTime 方法在 SmoothBursty 的实现中默认返回 0，
     * 即 SmoothBursty 的等待时间主要来自按照速率生成 freshPermits 个许可的时间，生成一个许可的时间为 stableIntervalMicros，
     * 故需要等待的时长为 freshPermits * stableIntervalMicros。
     *
     *  SmoothBursty#storedPermitsToWaitTime()：返回0
     *  SmoothWarmingUp#storedPermitsToWaitTime()：计算本次申请需要等待的时间，等待的时间由两部分组成，一部分是由 storedPermitsToWaitTime 方法返回的，
     *  另外一部分以稳定速率生成需要的许可，其需要时间为 freshPermits * stableIntervalMicros
     *
     */
    long waitMicros =
        storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend)
            + (long) (freshPermits * stableIntervalMicros);
    //更新 nextFreeTicketMicros 为当前时间加上需要等待的时间（freshPermits * stableIntervalMicros）。
    //如果requiredPermits < storedPermits  nextFreeTicketMicros不变
    //如果 requiredPermits >  storedPermits nextFreeTicketMicros延后
    this.nextFreeTicketMicros = LongMath.saturatedAdd(nextFreeTicketMicros, waitMicros);
    //减少本次已消耗的许可数量
    //如果 requiredPermits < storedPermits 则 storedPermits = storedPermits - requiredPermits
    //如果 requiredPermits >  storedPermits 则 storedPermits = 0
    this.storedPermits -= storedPermitsToSpend;
    /**
     * 请注意这里返回的 returnValue 的值，并没有包含由于剩余许可需要等待创建新许可的时间，即允许一定的突发流量，
     * 故本次计算需要的等待时间将对下一次请求生效，这也是框架作者将该限速器取名为 SmoothBursty 的缘由。
     */
    return returnValue;
  }

  /**
   * Translates a specified portion of our currently stored permits which we want to spend/acquire,
   * into a throttling time. Conceptually, this evaluates the integral of the underlying function we
   * use, for the range of [(storedPermits - permitsToTake), storedPermits].
   *
   * <p>This always holds: {@code 0 <= permitsToTake <= storedPermits}
   */
  abstract long storedPermitsToWaitTime(double storedPermits, double permitsToTake);

  /**
   * Returns the number of microseconds during cool down that we have to wait to get a new permit.
   */
  abstract double coolDownIntervalMicros();

  /**
   * Updates {@code storedPermits} and {@code nextFreeTicketMicros} based on the current time.
   */
  void resync(long nowMicros) {

    /**
     * 基于当前时间重置 SmoothRateLimiter 内部的 storedPermits (已存储的许可数量) 与
     * nextFreeTicketMicros (下一次可以免费获取许可的时间) 值，所谓的免费指的是无需等待就可以获取设定速率的许可，
     * 该方法对理解限流许可的产生非常关键，稍后详细介绍。
     */
    // if nextFreeTicket is in the past, resync to now
      //nextFreeTicketMicros：下一次可以免费获取许可的时间
    /**
     * 如果当前已启动时间大于 nextFreeTicketMicros（下一次可以免费获取许可的时间），则需要重新计算许可，即又可以向许可池中添加许可
     */
    if (nowMicros > nextFreeTicketMicros) {
      /**
       * 1、SmoothBursty 的 coolDownIntervalMicros() ：
       * 根据当前时间计算可增加的许可数量，在 SmoothBursty 的 coolDownIntervalMicros() 方法返回的是 stableIntervalMicros (发放一个许可所需要的时间)，
       * 故本次可以增加的许可数的算法也好理解，即用当前时间戳减去 nextFreeTicketMicros 的差值，再除以发送一个许可所需要的时间即可。
       *
       * 2、SmoothWarmingUp.coolDownIntervalMicros()：
       * 根据当前时间可增加的许可数量，由于 SmoothWarmingUp 实现了预热机制，平均生成一个许可的时间并不是固定不变的。
       *  用生成这些许可的总时间 除以  现在已经生成的许可数，即可得到当前时间点平均一个许可的生成时间
       *
       */
      //SmoothBursty.coolDownIntervalMicros() : 返回的是 stableIntervalMicros (发放一个许可所需要的时间)
      //SmoothWarmingUp.coolDownIntervalMicros()   : warmupPeriodMicros / maxPermits;
      double newPermits = (nowMicros - nextFreeTicketMicros) / coolDownIntervalMicros();
      //计算当前可用的许可，将新增的这些许可添加到许可池，但不会超过其最大值
      storedPermits = min(maxPermits, storedPermits + newPermits);
      //更新下一次可增加计算许可的时间
      //因为旧的nextFreeTicketMicros到nowMicros之间的许可已经增加了
      nextFreeTicketMicros = nowMicros;
    }
  }
}
