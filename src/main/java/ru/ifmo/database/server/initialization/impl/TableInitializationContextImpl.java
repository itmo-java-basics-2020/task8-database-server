package ru.ifmo.database.server.initialization.impl;

import lombok.Getter;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.SegmentReadResult;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private long prevLastOffset;
    private int currentIndex;
    private final HashMap<String, Segment> segments;

    public TableInitializationContextImpl(String tableName, Path tablePath, TableIndex tableIndex) {
        segments = new HashMap<>();
        initializationContexts = new ArrayList<>();
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.tableIndex = tableIndex;
        currentSegment = null;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Path getTablePath() {
        return tablePath;
    }

    @Override
    public TableIndex getTableIndex() {
        return tableIndex;
    }

    @Override
    public Segment getCurrentSegment() {
        return currentSegment;
    }

    @Override
    public void updateCurrentSegment(Segment segment) {
        currentSegment = segment;
    }

    @Override
    public Map<String, Segment> getSegments() {
        return segments;
    }

    @Override
    public Segment getSegment(String segmentName) {
        return segments.get(segmentName);
    }

    @Override
    public void addSegment(Segment segment) {
        segments.put(segment.getName(), segment);
        currentSegment = segment;
    }

    @Override
    public ArrayList<InitializationContext> getInitializationContexts() {
        return initializationContexts;
    }

    @Override
    public SegmentReadResult getPrevPart() {
        return prevPart;
    }

    @Override
    public void setPrevPart(SegmentReadResult prevPart) {
        this.prevPart = prevPart;
    }

    @Override
    public int getPrevIndex() {
        return prevIndex;
    }

    @Override
    public void setPrevIndex(int index) {
        prevIndex = index;
    }

    @Override
    public int getCurrentIndex() {
        return currentIndex;
    }

    @Override
    public void setCurrentIndex(int index) {
        currentIndex = index;
    }

    @Override
    public long getPrevOffset() {
        return prevLastOffset;
    }

    @Override
    public void setPrevOffset(long offset) {
        prevLastOffset = offset;
    }

}
