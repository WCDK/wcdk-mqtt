package com.wcdk.mqtt.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/***
* 测试集群有序任务队列：同一队列严格 FIFO、容量受限、关闭后拒绝新任务。
* @author wcdk
***/
class SerialTaskQueueTest {

    /***
    * 多线程执行体下，同一队列仍应按提交顺序串行执行。
    * @author wcdk
    ***/
    @Test
    void shouldPreserveSubmissionOrder() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            SerialTaskQueue queue = new SerialTaskQueue(executor, 1024, "test-order");
            List<Integer> order = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(200);
            for (int i = 0; i < 200; i++) {
                int value = i;
                assertThat(queue.submit(() -> {
                    order.add(value);
                    latch.countDown();
                })).isTrue();
            }
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(order).hasSize(200);
            for (int i = 0; i < 200; i++) {
                assertThat(order.get(i)).isEqualTo(i);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /***
    * 队列达到容量上限后应拒绝新任务，供调用方断链重同步。
    * @author wcdk
    ***/
    @Test
    void shouldRejectWhenCapacityExceeded() {
        // 执行体不运行任务，队列中的任务会持续累积到容量上限
        SerialTaskQueue queue = new SerialTaskQueue(runnable -> {
        }, 16, "test-capacity");
        for (int i = 0; i < 16; i++) {
            assertThat(queue.submit(() -> {
            })).isTrue();
        }
        assertThat(queue.submit(() -> {
        })).isFalse();
        assertThat(queue.size()).isEqualTo(16);
    }

    /***
    * 关闭队列后应拒绝新任务并丢弃未执行任务。
    * @author wcdk
    ***/
    @Test
    void shouldRejectAfterClose() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            SerialTaskQueue queue = new SerialTaskQueue(executor, 64, "test-close");
            queue.close();
            assertThat(queue.submit(() -> {
            })).isFalse();
            assertThat(queue.size()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }
}
