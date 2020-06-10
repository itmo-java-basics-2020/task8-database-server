package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.logic.Segment;

import java.util.TreeMap;

public class SegmentIndex extends AbstractDatabaseIndex<String, SegmentIndexInfo> {
    public SegmentIndex() {
        map = new TreeMap<>();
    }
}
