package com.zhisheng.data.sinks.sinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author fanrui
 * @date 2019-11-18 00:12:38
 */
public class MultiThreadConsumerClient implements Runnable {

    private Logger LOG = LoggerFactory.getLogger(MultiThreadConsumerClient.class);

    private LinkedBlockingQueue<String> bufferQueue;
    private CyclicBarrier barrier;

    public MultiThreadConsumerClient(
            LinkedBlockingQueue<String> bufferQueue, CyclicBarrier barrier) {
        this.bufferQueue = bufferQueue;
        this.barrier = barrier;
    }


    @Override
    public void run() {
        String entity;
        while (true){
            try {
                // 从 bufferQueue 的队首消费数据，并设置 timeout
                entity = bufferQueue.poll(50, TimeUnit.MILLISECONDS);
                // entity != null 表示 bufferQueue 有数据
                if(entity != null){
                    // 执行 client 消费数据的逻辑
                    doSomething(entity);
                } else {
                    // entity == null 表示 bufferQueue 中已经没有数据了，
                    // 且 barrier wait 大于 0 表示当前正在执行 Checkpoint，
                    // client 需要执行 flush，保证 Checkpoint 之前的数据都消费完成
                    if ( barrier.getNumberWaiting() > 0 ) {
                        LOG.info("MultiThreadConsumerClient 执行 flush, " +
                                "当前 wait 的线程数：" + barrier.getNumberWaiting());
                        flush();
                        // barrier 等待
                        barrier.await();
                    }
                }
            } catch (InterruptedException| BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }


    // client 消费数据的逻辑
    private void doSomething(String entity) {

    }


    // client 执行 flush 操作，防止丢数据
    private void flush() {
        // client.flush();
    }
}
