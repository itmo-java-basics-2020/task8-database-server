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
        if (context.currentSegmentContext() == null) {
            throw new DatabaseException("Context segment is null");
        }
        Segment segment = new SegmentImpl(context.currentSegmentContext());
        context.currentTableContext().updateCurrentSegment(segment);

        File file = new File(context.currentSegmentContext().getSegmentPath().toString());
        try {
            DatabaseInputStream init = new DatabaseInputStream(new FileInputStream(file));


            for (int i = 0; i < file.length();) {
                Optional<DatabaseStoringUnit> dbUnit = init.readDbUnit(0);

                if (dbUnit.isEmpty()) {
                    throw new DatabaseException("Dbunit is Empty");
                }

                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(new String(dbUnit.get().getKey()), new SegmentIndexInfoImpl(i));

                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(new String(dbUnit.get().getKey()), segment);

                i += dbUnit.get().getUnitSize();
            }
        } catch (IOException e) {
            throw new DatabaseException(e);
        }
    }
}
