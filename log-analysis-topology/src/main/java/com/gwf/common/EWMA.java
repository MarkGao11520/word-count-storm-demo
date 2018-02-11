package com.gwf.common;

import java.io.Serializable;

public class EWMA implements Serializable {
    private static final long serialVersionUID = -6408346318181111576L;
    // 和UNIX系统计算负载时使用的标准alpha值相同
    public static final double ONE_MINUTE_ALPHA = 1-Math.exp(-5d / 60d / 1d);
    public static final double FIVE_MINUTE_ALPHA = 1-Math.exp(-5d / 60d / 5d);
    public static final double FIFTEEN_MINUTE_ALPHA = 1-Math.exp(-5d / 60d / 15d);

    public static enum Time {
        MILLISECONDS(1),
        SECONDS(1000),
        MINUTES(SECONDS.getMillis() * 60),
        HOURS(MINUTES.getMillis() * 60),
        DAYS(HOURS.getMillis() * 24),
        WEEKS(DAYS.getMillis() * 7);

        private long millis;

        Time(long millis) {
            this.millis = millis;
        }

        public long getMillis() {
            return millis;
        }
    }

    private long window;  //滑动窗口大小
    private long alphaWindow;
    private long last;  //记录上一次的时间
    private double average;  //移动平均值
    private double alpha = -1D;  //平滑水平
    private boolean sliding = false;  //是否移动

    public EWMA() {
    }

    /**
     * 建立指定时间的滑动窗口
     */
    public EWMA sliding(double count,Time time){
        return this.sliding((long)(time.getMillis()*count));
    }

    private EWMA sliding(long window){
        this.sliding = true;
        this.window = window;
        return this;
    }

    /**
     * 指定alpha值
     * @param alpha
     * @return
     */
    public EWMA withAlpha(double alpha){
        if(!(alpha>0.0D)&&alpha<=1.0D){
            throw new IllegalArgumentException("Alpha must be between 0.0 and 1.0");
        }
        this.alpha = alpha;
        return this;
    }

    /**
     * 作为一个alphaWindow窗口的函数
     * alpha = 【1-Math.exp(-5d / 60d / alphaWindow)】
     * @param alphaWindow
     * @return
     */
    public EWMA withAlphaWindow(long alphaWindow){
        this.alpha = -1;
        this.alphaWindow = alphaWindow;
        return this;
    }


    public EWMA withAlphaWindow(double count,Time time){
        return this.withAlphaWindow((long)(time.getMillis()*count));
    }

    /**
     * 默认使用当前时间更新移动平均值
     */
    public void mark(){
        mark(System.currentTimeMillis());
    }

    /**
     * 更新移动平均值
     * @param time
     */
    public synchronized void mark(long time){
        if(this.sliding){
            //如果发生时间间隔大于窗口，则重置滑动窗口
            if(time-this.last > this.window){
                this.last = 0;
            }
        }
        if(this.last == 0){
            this.average = 0;
            this.last = time;
        }
        // 计算上一次和本次的时间差
        long diff = time-this.last;
        // 计算alpha
        double alpha = this.alpha != -1.0 ? this.alpha : Math.exp(-1.0*((double)diff/this.alphaWindow));
        // 计算当前平均值
        this.average = (1.0-alpha)*diff + alpha*this.average;
        this.last = time;
    }

    /**
     * mark()方法多次调用的平均间隔时间（历史平均水平）
     * @return
     */
    public double getAverage(){
        return this.average;
    }

    /**
     * 按照指定的时间返回平均值
     * @param time
     * @return
     */
    public double getAverageIn(Time time){
        return this.average == 0.0?this.average:this.average/time.getMillis();
    }

    /**
     * 返回特定时间度量内调用mark()的频率
     * @param time
     * @return
     */
    public double getAverageRatePer(Time time){
        return this.average == 0.0?this.average:time.getMillis()/this.average;
    }
}
