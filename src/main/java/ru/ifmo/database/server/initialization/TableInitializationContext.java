package ru.ifmo.database.server.initialization;

import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.nio.file.Path;
import java.util.Map;

public interface TableInitializationContext {
    String getTableName();

    Path getTablePath();

    Path getDatabasePath();

    TableIndex getTableIndex();

    Segment getCurrentSegment();

    void addSegment(String segmentName, Segment segment);

    Segment getSegment(String segmentName);

    Map<String, Segment> getSegments();

    void updateCurrentSegment(Segment segment);
}
