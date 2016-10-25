package com.mistraltech.jpaapplication;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = JpaApplication.class)
public class NonRepeatableReadTests {

    @Autowired
    public ItemProcessor itemProcessor;

    private ExecutorService executorService;

    @Before
    public void setUp() {
        executorService = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void disallowsNonRepeatableReads_ReadWithReadCommitted_CreateWithReadCommitted() throws Exception {
        BiFunction<CyclicBarrier, Long, Boolean> reader = itemProcessor::checkForRepeatableReadSafetyWithReadCommittedIsolation;
        Consumer<Long> updater = itemProcessor::updateItemWithReadCommittedIsolation;
        executeRepeatableReadSafetyCheck(reader, updater);
    }

    @Test
    public void disallowsNonRepeatableReads_ReadWithReadCommitted_CreateWithReadUncommitted() throws Exception {
        BiFunction<CyclicBarrier, Long, Boolean> reader = itemProcessor::checkForRepeatableReadSafetyWithReadCommittedIsolation;
        Consumer<Long> updater = itemProcessor::updateItemWithReadUncommittedIsolation;
        executeRepeatableReadSafetyCheck(reader, updater);
    }

    @Test
    public void disallowsNonRepeatableReads_ReadWithSerializable_CreateWithReadCommitted() throws Exception {
        BiFunction<CyclicBarrier, Long, Boolean> reader = itemProcessor::checkForRepeatableReadSafetyWithSerializableIsolation;
        Consumer<Long> updater = itemProcessor::updateItemWithReadCommittedIsolation;
        executeRepeatableReadSafetyCheck(reader, updater);
    }

    @Test
    public void disallowsNonRepeatableReads_ReadWithSerializable_CreateWithReadUncommitted() throws Exception {
        BiFunction<CyclicBarrier, Long, Boolean> reader = itemProcessor::checkForRepeatableReadSafetyWithSerializableIsolation;
        Consumer<Long> updater = itemProcessor::updateItemWithReadUncommittedIsolation;
        executeRepeatableReadSafetyCheck(reader, updater);
    }

    private void executeRepeatableReadSafetyCheck(BiFunction<CyclicBarrier, Long, Boolean> reader, Consumer<Long> updater) throws Exception {
        // We are going to use a two-thread cyclic barrier to synchronise two concurrent transactions
        CyclicBarrier syncBarrier = new CyclicBarrier(2);

        long itemId = itemProcessor.createItemWithReadCommittedIsolation();

        // Start a read transaction in another thread and wait for it to do initial read-all
        Future<Boolean> readerFuture = executorService.submit(() -> reader.apply(syncBarrier, itemId));

        syncBarrier.await(1, TimeUnit.SECONDS);

        // Attempt to add a new item in a write transaction
        try {
            updater.accept(itemId);
        } catch (ConcurrencyFailureException e) {
            // Unable to update item while read transaction is in progress means is a test pass
            return;
        }

        // Notify the read transaction that the has update completed
        try {
            syncBarrier.await(1, TimeUnit.SECONDS);
        } catch (BrokenBarrierException e) {
            // This will happen if the reader thread times out waiting for the barrier, which means the updater
            // was blocked until the reader exited, which is also a test pass
            return;
        }

        boolean repeatableRead = readerFuture.get();

        assertTrue("Repeated reads should return consistent results", repeatableRead);
    }
}
