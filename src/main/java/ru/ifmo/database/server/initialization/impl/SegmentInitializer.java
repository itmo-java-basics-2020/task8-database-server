package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.DatabaseStoringUnit;
import ru.ifmo.database.server.logic.impl.SegmentImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        SegmentInitializationContext segmentContext = context.currentSegmentContext();
        if (segmentContext == null) {
            throw new DatabaseException("Segment context is null");
        }

        File file = segmentContext.getSegmentPath().toFile();

        Segment segment = new SegmentImpl(segmentContext);

        int index = 0;
        try (DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(file))) {
            while (index < file.length()) {
                Optional<DatabaseStoringUnit> unit = databaseInputStream.readDbUnit(0);

                segmentContext.getIndex().onIndexedEntityUpdated(
                        new String(unit.get().getKey()),
                        new SegmentIndexInfoImpl(index));

                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(
                        new String(unit.get().getKey()),
                        segment);

                index += unit.get().getUnitSize();
            }
        } catch (IOException | NoSuchElementException e) {
            throw new DatabaseException(e.getMessage());
        }
    }
}
