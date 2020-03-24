package com.persequor.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Event {
    private UUID id;
    private LocalDateTime createdAt;
    private String source;
    private String action;
    private List<String> trackedIds = new ArrayList<>();

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public List<String> getTrackedIds() {
        return trackedIds;
    }

    public void setTrackedIds(List<String> trackedIds) {
        this.trackedIds = trackedIds;
    }
}
