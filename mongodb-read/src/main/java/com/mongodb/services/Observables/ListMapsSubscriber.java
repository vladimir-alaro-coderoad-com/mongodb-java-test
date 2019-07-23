package com.mongodb.services.Observables;

import java.util.Map;

public class ListMapsSubscriber extends OperationSubscriber<Map<String, Object>> {
    @Override
    public void onNext(Map<String, Object> map) {
        super.onNext(map);
    }
}
