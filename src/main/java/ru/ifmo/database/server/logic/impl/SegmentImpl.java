package ru.ifmo.database.server.logic.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.SegmentIndexInfo;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;

import java.io.*;
import java.nio.file.Path;
import java.util.Optional;

public class SegmentImpl implements Segment {

    private static final Integer DEFAULT_SEGMENT_SIZE = 100_000;

    private final String name;
    private final Path tableRootPath;
    private final SegmentIndex segmentIndex;

    private boolean isReadOnly;
    private long currentSize;

    public SegmentImpl(SegmentInitializationContext context) {
        name = context.getSegmentName();
        currentSize = context.getCurrentSize();
        tableRootPath = context.getSegmentPath().getParent();
        isReadOnly = context.isSegmentReadOnly();
        segmentIndex = context.getIndex();
    }

    private SegmentImpl(String segmentName, Path tableRootPath) throws DatabaseException {
        this.currentSize = 0;
        this.name = segmentName;
        this.isReadOnly = false;
        this.tableRootPath = tableRootPath;
        this.segmentIndex = new SegmentIndex();

        try {
            new File(tableRootPath.toString(), name).createNewFile();
        } catch (IOException e) {
            throw new DatabaseException("Some exceptions with files: " + e.getMessage());
        }
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        return new SegmentImpl(segmentName, tableRootPath);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    private DatabaseOutputStream getOutputStream() throws FileNotFoundException {
        return new DatabaseOutputStream(new FileOutputStream(new File(tableRootPath.toString(), name), true));
    }

    private DatabaseInputStream getInputStream() throws FileNotFoundException {
        return new DatabaseInputStream(new FileInputStream(new File(tableRootPath.toString(), name)));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean write(String objectKey, String objectValue) throws IOException {
        DatabaseStoringUnit data = new DatabaseStoringUnit(objectKey, objectValue);

        if (data.getSizeInFile() + currentSize > DEFAULT_SEGMENT_SIZE) {
            isReadOnly = true;
            return false;
        }

        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentIndexInfoImpl(currentSize));
        currentSize += getOutputStream().write(data);
        return true;
    }

    @Override
    public String read(String objectKey) throws DatabaseException {
        Optional<SegmentIndexInfo> oOffset = segmentIndex.searchForKey(objectKey);

        if (oOffset.isEmpty()) {
            throw new DatabaseException("Internal error. TableIndex has key " + objectKey +
                    ", but SegmentIndex doesn't have");
        }

        try (DatabaseInputStream in = getInputStream()) {
            Optional<DatabaseStoringUnit> oData = in.readDbUnit(oOffset.get().getOffset());

            if (oData.isEmpty()) {
                throw new DatabaseException("Some exceptions with files");
            }

            return new String(oData.get().getValue());
        } catch (IOException e) {
            throw new DatabaseException("Internal error. Somebody changed file");
        }
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }
}
