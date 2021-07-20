package com.github.spartatech.sqljson.vo;

import java.util.ArrayList;
import java.util.List;

public class MultiColumn {
    private MultiListType filterType;
    private List<Object> items = new ArrayList<>();

    public MultiColumn(MultiListType filterType) {
        this.filterType = filterType;
    }

    public void addItem(Object item) {
        items.add(item);
    }

    public void addItems(List<Object> items) {
        this.items.addAll(items);
    }

    public MultiListType getFilterType() {
        return filterType;
    }

    public List<Object> getItems() {
        return items;
    }

    @Override
    public String toString() {
        return "MultiColumn{" +
                "filterType=" + filterType +
                ", items=" + items +
                '}';
    }
}
