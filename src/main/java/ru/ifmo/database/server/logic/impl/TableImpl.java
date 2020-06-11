package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {

    private static final Integer MAX_SEGMENT_SIZE = 100_000;

    private final String tableName;
    private final Path pathToDatabaseRoot;
    private final TableIndex tableIndex;
    private Segment currentSegment;
    private final HashMap<String, Segment> tableSegments;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) {
        try {
            if (!Files.exists(Path.of(pathToDatabaseRoot.toString() + "\\" + tableName))) {
                Files.createDirectory(Path.of(pathToDatabaseRoot.toString() + "\\" + tableName));
            }
        } catch (IOException e) {
            System.out.println(tableName + " already exist");
        } finally {
            this.tableName = tableName;
            this.pathToDatabaseRoot = pathToDatabaseRoot;
            this.tableIndex = tableIndex;
            this.tableSegments = new HashMap<>();
            createNewSegment();
        }
    }

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex, Map<String, Segment> tableSegments) {
        try {
            if (!Files.exists(Path.of(pathToDatabaseRoot.toString() + "\\" + tableName))) {
                Files.createDirectory(Path.of(pathToDatabaseRoot.toString() + "\\" + tableName));
            }
        } catch (IOException e) {
            System.out.println(tableName + " already exist");
        } finally {
            this.tableName = tableName;
            this.pathToDatabaseRoot = pathToDatabaseRoot;
            this.tableIndex = tableIndex;
            this.tableSegments = (HashMap<String, Segment>) tableSegments;
            for (Segment segment : tableSegments.values()) {
                if (!segment.isReadOnly()) {
                    currentSegment = segment;
                }
            }
        }
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return new TableImpl(tableName, pathToDatabaseRoot, tableIndex);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return new TableImpl(context.getTableName(), context.getDatabasePath(), context.getTableIndex(), context.getSegments());
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void write(String objectKey, String objectValue) throws DatabaseException {
        if (currentSegment.getSegmentSize() + (objectKey + objectValue).getBytes().length > MAX_SEGMENT_SIZE) {
            currentSegment.turnToReadOnly();
            createNewSegment();
        }
        currentSegment.write(objectKey, objectValue);
        tableIndex.updateSegment(objectKey, currentSegment);
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        if (!tableIndex.ifContains(objectKey)) {
            throw new DatabaseException("No such key");
        } else {
            return tableIndex.getSegment(objectKey).read(objectKey);
        }
    }

    @Override
    public void addSegment(String segmentName, Segment segment) {
        if (!tableSegments.containsKey(segmentName)) {
            tableSegments.put(segmentName, segment);
        }
    }

    @Override
    public Segment getSegment(String segmentName) {
        if (tableSegments.containsKey(segmentName)) {
            return tableSegments.get(segmentName);
        }
        return null;
    }

    private void createNewSegment() {
        try {
            currentSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName),
                    Path.of(pathToDatabaseRoot.toString() + "\\" + tableName));
            addSegment(currentSegment.getName(), currentSegment);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
}