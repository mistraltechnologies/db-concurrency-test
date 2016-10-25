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
import java.util.function.Function;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = JpaApplication.class)
public class PhantomReadTests {

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
    public void disallowsPhantomReads_ReadWithReadCommitted_CreateWithReadCommitted() throws Exception {
        Function<CyclicBarrier, Boolean> reader = itemProcessor::checkForPhantomReadSafetyWithReadCommittedIsolation;
        Runnable updater = itemProcessor::createItemWithReadCommittedIsolation;
        executePhantomReadSafetyCheck(reader, updater);
    }

    @Test
    public void disallowsPhantomReads_ReadWithReadCommitted_CreateWithReadUncommitted() throws Exception {
        Function<CyclicBarrier, Boolean> reader = itemProcessor::checkForPhantomReadSafetyWithReadCommittedIsolation;
        Runnable updater = itemProcessor::createItemWithReadUncommittedIsolation;
        executePhantomReadSafetyCheck(reader, updater);
    }

    @Test
    public void disallowsPhantomReads_ReadWithSerializable_CreateWithReadCommitted() throws Exception {
        Function<CyclicBarrier, Boolean> reader = itemProcessor::checkForPhantomReadSafetyWithSerializableIsolation;
        Runnable updater = itemProcessor::createItemWithReadCommittedIsolation;
        executePhantomReadSafetyCheck(reader, updater);
    }

    @Test
    public void disallowsPhantomReads_ReadWithSerializable_CreateWithReadUncommitted() throws Exception {
        Function<CyclicBarrier, Boolean> reader = itemProcessor::checkForPhantomReadSafetyWithSerializableIsolation;
        Runnable updater = itemProcessor::createItemWithReadUncommittedIsolation;
        executePhantomReadSafetyCheck(reader, updater);
    }

    private void executePhantomReadSafetyCheck(Function<CyclicBarrier, Boolean> reader, Runnable creator) throws Exception {
        // We are going to use a two-thread cyclic barrier to synchronise two concurrent transactions
        CyclicBarrier syncBarrier = new CyclicBarrier(2);

        // Start a read transaction in another thread and wait for it to do initial read-all
        Future<Boolean> readerFuture = executorService.submit(() -> reader.apply(syncBarrier));

        syncBarrier.await(1, TimeUnit.SECONDS);

        // Attempt to add a new item in a write transaction and notify the read transaction
        //noinspection TryWithIdenticalCatches
        try {
            creator.run();
        } catch (ConcurrencyFailureException e) {
            // Unable to write new item while read transaction is in progress means is a test pass
            return;
        }

        // Notify the read transaction that the has create completed
        try {
            syncBarrier.await(1, TimeUnit.SECONDS);
        } catch (BrokenBarrierException e) {
            // This will happen if the reader thread times out waiting for the barrier, which means the creator
            // was blocked until the reader exited, which is also a test pass
            return;
        }

        boolean repeatableRead = readerFuture.get();

        assertTrue("Repeated reads should return consistent results", repeatableRead);
    }
}
