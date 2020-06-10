package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.DatabaseStoringUnit;
import ru.ifmo.database.server.logic.impl.SegmentImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        Segment segment = new SegmentImpl(context.currentSegmentContext());
        context.currentTableContext().updateCurrentSegment(segment);

        File file = new File(context.currentSegmentContext().getSegmentPath().toString());
        try {
            DatabaseInputStream init = new DatabaseInputStream(new FileInputStream(file));

            int pos = 0;
            while (pos != file.length()) {
                Optional<DatabaseStoringUnit> dbUnit = init.readDbUnit(0);

                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(new String(dbUnit.get().getKey()), new SegmentIndexInfoImpl(pos));

                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(new String(dbUnit.get().getKey()), segment);
            }
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }
}
