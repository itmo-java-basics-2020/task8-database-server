package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.DatabaseStoringUnit;
import ru.ifmo.database.server.logic.impl.SegmentImpl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class SegmentInitializer implements Initializer {

    public SegmentInitializer() {

    }

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        TableInitializationContext tableContext = context.currentTableContext();
        SegmentInitializationContext segmentContext = context.currentSegmentContext();

        SegmentIndex segmentIndex = segmentContext.getIndex();
        try (DatabaseInputStream inputStream = new DatabaseInputStream(
                Files.newInputStream(segmentContext.getSegmentPath(), StandardOpenOption.READ))) {
            while (inputStream.available() > 0) {
                int offset = segmentContext.getCurrentSize() - inputStream.available();
                DatabaseStoringUnit inputUnit = inputStream.readDbUnit().get();
                String key = new String(inputUnit.getKey());
                segmentIndex.onIndexedEntityUpdated(
                        key,
                        new SegmentIndexInfoImpl(offset));
            }
        } catch (IOException ex) {
            throw new DatabaseException(ex);
        }
        Segment segment = new SegmentImpl(segmentContext);
        TableIndex tableIndex = tableContext.getTableIndex();
        for (String key : segmentIndex.getKeys()) {
            tableIndex.onIndexedEntityUpdated(key, segment);
        }
        tableContext.updateCurrentSegment(segment);
    }
}
