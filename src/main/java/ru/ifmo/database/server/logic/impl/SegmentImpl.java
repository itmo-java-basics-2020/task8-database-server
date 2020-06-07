package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {

    private boolean isReadOnly;
    private final String segmentName;
    private final Path tableRootPath;
    private final SegmentIndex segmentIndex;
    private Integer segmentSize;

    private SegmentImpl(String segmentName, Path tableRootPath, SegmentIndex segmentIndex) {
        try {
            if (!Files.exists(Path.of(tableRootPath.toString() + "\\" + segmentName))) {
                Files.createFile(Path.of(tableRootPath.toString() + "\\" + segmentName));
                segmentSize = 0;
            } else {
                segmentSize = (int) new File(tableRootPath.toString() + "\\" + segmentName).length();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.segmentName = segmentName;
            this.tableRootPath = tableRootPath;
            this.segmentIndex = segmentIndex;
        }
    }

    public static SegmentImpl initializeFromContext(SegmentInitializationContext context) {
        return new SegmentImpl(context.getSegmentName(), context.getTableRootPath(), context.getIndex());
    }

    public static SegmentImpl create(String segmentName, Path tableRootPath) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath, new SegmentIndex());
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws DatabaseException {
        boolean answer;
        try {
            answer = (new DatabaseOutputStream(new FileOutputStream(tableRootPath.toString() + "\\" + segmentName, true))
                    .write(new DatabaseStoringUnit(objectKey, objectValue))) == 1;
        } catch (IOException e) {
            throw new DatabaseException("No such segment");
        }
        return answer;
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        if (!segmentIndex.ifContains(objectKey)) {
            throw new DatabaseException("No such key");
        } else {
            int offset = segmentIndex.getSegmentIndexInfo(objectKey).getOffset();
            String value;
            try (DatabaseInputStream in = new DatabaseInputStream(new FileInputStream(tableRootPath + "\\" + segmentName))) {
                DatabaseStoringUnit dbStoringUnit = in.readDbUnit(offset);
                if (dbStoringUnit == null) {
                    throw new DatabaseException("No value found");
                }
                value = new String(dbStoringUnit.getValue());
                return value;
            } catch (IOException e) {
                throw new DatabaseException("No value connected to this key");
            }
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    @Override
    public void turnToReadOnly() {
        isReadOnly = true;
    }

    @Override
    public Integer getSegmentSize() {
        return segmentSize;
    }
}
