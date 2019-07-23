package com.mongodb.services.Observables;

import com.mongodb.MongoTimeoutException;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ObservableSubscriber<T> implements Subscriber<T> {

    private final List<T> results;
    private final List<Throwable> errors;
    private final CountDownLatch latch;
    private volatile Subscription subscription;
    private volatile boolean completed;

    public ObservableSubscriber() {
        this.results = new ArrayList<>();
        this.errors = new ArrayList<>();
        this.latch = new CountDownLatch(1);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
    }

    @Override
    public void onNext(T t) {
        results.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
        errors.add(throwable);
        onComplete();
    }

    @Override
    public void onComplete() {
        completed = Boolean.TRUE;
        latch.countDown();
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public List<T> getResults() {
        return results;
    }

    public Throwable getError() {
        if (errors.size() > 0) {
            return errors.get(0);
        }
        return null;
    }

    public boolean isCompleted() {
        return completed;
    }

    public List<T> get(final long timeout, final TimeUnit unit) throws Throwable {
        return await(timeout, unit).getResults();
    }

    public ObservableSubscriber<T> await() throws Throwable {
        return await(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) throws Throwable {
        subscription.request(Integer.MAX_VALUE);
        if (!latch.await(timeout, unit)) {
            throw new MongoTimeoutException("Publisher onComplete timed out");
        }
        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
        return this;
    }
}
