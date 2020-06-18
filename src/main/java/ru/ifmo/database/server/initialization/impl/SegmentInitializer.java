package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndex;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.index.impl.TableIndex;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.DatabaseStoringUnit;
import ru.ifmo.database.server.logic.impl.SegmentImpl;


import java.io.IOException;
import java.nio.file.Files;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        SegmentIndex segmentIndex = context.currentSegmentContext().getIndex();
        Segment segment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
        TableIndex tableIndex = context.currentTableContext().getTableIndex();

        try {
            DatabaseInputStream inputStream = new DatabaseInputStream(Files.newInputStream(context.currentSegmentContext().getSegmentPath()));
            while (inputStream.available() > 0) {
                int offset = context.currentSegmentContext().getCurrentSize() - inputStream.available();
                DatabaseStoringUnit databaseStoringUnit = inputStream.readDbUnit().get();
                segmentIndex.onIndexedEntityUpdated(new String(databaseStoringUnit.getKey()), new SegmentIndexInfoImpl(offset));
                tableIndex.onIndexedEntityUpdated(new String(databaseStoringUnit.getKey()), segment);
            }
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

    }
}
