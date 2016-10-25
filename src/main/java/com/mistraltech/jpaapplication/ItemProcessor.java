package com.mistraltech.jpaapplication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

@Component
public class ItemProcessor {
    private final ItemRepository itemRepository;

    private Logger log = LoggerFactory.getLogger(ItemProcessor.class);

    @Autowired
    public ItemProcessor(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public long createItemWithReadUncommittedIsolation() {
        return createItem();
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public long createItemWithReadCommittedIsolation() {
        return createItem();
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public void updateItemWithReadUncommittedIsolation(long id) {
        updateItem(id);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void updateItemWithReadCommittedIsolation(long id) {
        updateItem(id);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public boolean checkForPhantomReadSafetyWithSerializableIsolation(CyclicBarrier syncBarrier) {
        return checkForPhantomReadSafety(syncBarrier);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public boolean checkForPhantomReadSafetyWithReadCommittedIsolation(CyclicBarrier syncBarrier) {
        return checkForPhantomReadSafety(syncBarrier);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public boolean checkForRepeatableReadSafetyWithSerializableIsolation(CyclicBarrier syncBarrier, long id) {
        return checkForRepeatableReadSafety(syncBarrier, id);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public boolean checkForRepeatableReadSafetyWithReadCommittedIsolation(CyclicBarrier syncBarrier, long id) {
        return checkForRepeatableReadSafety(syncBarrier, id);
    }

    private boolean checkForPhantomReadSafety(CyclicBarrier syncBarrier) {
        long originalCount = itemRepository.count();
        log.info("Original count: " + originalCount);

        // Signal that we have completed our initial read...
        sync(syncBarrier);

        // Wait to be signalled that a new item has been added...
        sync(syncBarrier);

        long newCount = itemRepository.count();
        log.info("New count: " + newCount);

        return newCount == originalCount;
    }

    private boolean checkForRepeatableReadSafety(CyclicBarrier syncBarrier, long id) {
        int originalValue = findItemFromAll(id).getValue();
        log.info("Original value: " + originalValue);

        // Signal that we have completed our initial read...
        sync(syncBarrier);

        // Wait to be signalled that the item has been updated...
        sync(syncBarrier);

        int newValue = findItemFromAll(id).getValue();
        log.info("New value: " + newValue);

        return newValue == originalValue;
    }

    private long createItem() {
        ItemEntity item = new ItemEntity();
        itemRepository.save(item);
        log.info("Created item: " + item);
        return item.getId();
    }

    private void updateItem(long id) {
        ItemEntity item = itemRepository.findOne(id);
        item.incrementValue();
        log.info("Updated item: " + item);
    }

    private void sync(CyclicBarrier syncBarrier) {
        try {
            syncBarrier.await(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ItemEntity findItemFromAll(long id) {
        Iterable<ItemEntity> allItems = itemRepository.findAll();
        return StreamSupport.stream(allItems.spliterator(), false)
                .filter(i -> i.getId() == id)
                .findAny()
                .orElseThrow(() -> new RuntimeException("Failed to find item with id " + id));
    }
}
