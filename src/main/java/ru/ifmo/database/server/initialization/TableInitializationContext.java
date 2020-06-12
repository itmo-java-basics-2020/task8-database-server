package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.SegmentReadResult;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

public interface TableInitializationContext {
    String getTableName();

    Path getTablePath();

    TableIndex getTableIndex();

    Segment getCurrentSegment();

    Segment getSegment(String segmentName);

    void addSegment(Segment segment);

    ArrayList<InitializationContext> getInitializationContexts();

    SegmentReadResult getPrevPart();

    void setPrevPart(SegmentReadResult prevPart);

    int getPrevIndex();

    void setPrevIndex(int index);

    int getCurrentIndex();

    long getPrevOffset();

    void setPrevOffset(long offset);
}
