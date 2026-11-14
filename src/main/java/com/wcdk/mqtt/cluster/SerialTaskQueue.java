package com.wcdk.mqtt.cluster;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
* 每个集群连接独立的有序任务队列：任务按提交顺序串行执行，执行体由共享线程池承载。
* 用于把 JSON/Base64 编解码移出 Reactor 事件循环，同时保持同一连接的报文处理顺序。
* @author wcdk
***/
final class SerialTaskQueue {

    private static final Logger log = LoggerFactory.getLogger(SerialTaskQueue.class);

    private final Executor executor;
    private final int capacity;
    private final String name;
    private final Deque<Runnable> tasks = new ArrayDeque<>();
    /** 是否有工作线程正在排空队列 */
    private boolean draining;
    /** 队列是否已关闭 */
    private boolean closed;

    SerialTaskQueue(Executor executor, int capacity, String name) {
        this.executor = executor;
        this.capacity = Math.max(16, capacity);
        this.name = name;
    }

    /***
    * 提交一个有序任务，队列满或已关闭时返回 false。
    * @author wcdk
    * @param task 待执行任务
    * @return 提交成功返回 true
    ***/
    boolean submit(Runnable task) {
        if (task == null) {
            return false;
        }
        synchronized (this) {
            if (closed) {
                return false;
            }
            if (tasks.size() >= capacity) {
                return false;
            }
            tasks.addLast(task);
            if (draining) {
                return true;
            }
            draining = true;
        }
        try {
            executor.execute(this::drain);
        } catch (RejectedExecutionException ex) {
            synchronized (this) {
                draining = false;
            }
            log.warn("集群有序任务队列提交失败，队列：{}，异常信息：{}", name, ex.getMessage());
            return false;
        }
        return true;
    }

    /***
    * 排空队列，每个任务按提交顺序串行执行。
    * @author wcdk
    ***/
    private void drain() {
        while (true) {
            Runnable task;
            synchronized (this) {
                if (closed) {
                    tasks.clear();
                    draining = false;
                    return;
                }
                task = tasks.pollFirst();
                if (task == null) {
                    draining = false;
                    return;
                }
            }
            try {
                task.run();
            } catch (RuntimeException ex) {
                log.warn("集群有序任务执行失败，队列：{}，异常信息：{}", name, ex.getMessage());
            }
        }
    }

    /***
    * 返回当前排队任务数，用于诊断。
    * @author wcdk
    * @return 排队任务数
    ***/
    int size() {
        synchronized (this) {
            return tasks.size();
        }
    }

    /***
    * 关闭队列并丢弃未执行任务。
    * @author wcdk
    ***/
    void close() {
        synchronized (this) {
            closed = true;
            tasks.clear();
        }
    }
}
