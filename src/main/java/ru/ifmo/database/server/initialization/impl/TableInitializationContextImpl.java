package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.SegmentReadResult;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Getter
public class TableInitializationContextImpl implements TableInitializationContext {

    private final String tableName;
    private final Path tablePath;
    private final TableIndex tableIndex;
    private Segment currentSegment;
    private final ArrayList<InitializationContext> initializationContexts;
    private SegmentReadResult prevPart;
    private int prevIndex;
    private long prevOffset;
    private int currentIndex;
    private final Map<String, Segment> segments;

    public TableInitializationContextImpl(String tableName, Path tablePath, TableIndex tableIndex) {
        segments = new HashMap<>();
        initializationContexts = new ArrayList<>();
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        currentSegment = null;
        currentIndex = -1;
    }


    @Override
    public Segment getSegment(String segmentName) {
        return segments.get(segmentName);
    }

    @Override
    public void addSegment(Segment segment) {
        segments.put(segment.getName(), segment);
        currentSegment = segment;
        currentIndex++;
    }

    @Override
    public void setPrevPart(SegmentReadResult prevPart) {
        this.prevPart = prevPart;
    }

    @Override
    public void setPrevIndex(int index) {
        prevIndex = index;
    }


    @Override
    public void setPrevOffset(long offset) {
        prevOffset = offset;
    }

}
