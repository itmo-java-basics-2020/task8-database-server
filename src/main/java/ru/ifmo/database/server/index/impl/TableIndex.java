package ru.ifmo.database.server.index.impl;

import ru.ifmo.database.server.index.AbstractDatabaseIndex;
import ru.ifmo.database.server.logic.Segment;

import java.util.Optional;

public class TableIndex extends AbstractDatabaseIndex<String, Segment> {

    public Optional<Segment> next(String currentSegmentName) {
        return Optional.ofNullable(map.higherEntry(currentSegmentName).getValue());
    }
}
