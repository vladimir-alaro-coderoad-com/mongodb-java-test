package com.mongodb.services.Observables;

public class SingleResultSubscriber<T> extends OperationSubscriber<T> {

    private T result;

    public SingleResultSubscriber(T result) {
        this.result = result;
    }

    @Override
    public void onComplete() {
        if (!getResults().isEmpty()) {
            result = getResults().get(0);
        }
        super.onComplete();
    }

    public T getResult() {
        return result;
    }
}
