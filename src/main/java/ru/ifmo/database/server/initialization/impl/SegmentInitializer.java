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
            throw new DatabaseException("Segment skipped context during initialization");
        }

        File file = context.currentSegmentContext().getSegmentPath().toFile();

        Segment segment = new SegmentImpl(context.currentSegmentContext());

        int index = 0;
        try (DatabaseInputStream databaseInputStream = new DatabaseInputStream(new FileInputStream(file))) {
            while (index < file.length()) {
                for (int i = 0; i < file.length(); ) {
                    Optional<DatabaseStoringUnit> unit = databaseInputStream.readDbUnit(0);

                    if (unit.isEmpty()) {
                        throw new DatabaseException("Unit is Empty");
                    }

                    context.currentSegmentContext().getIndex().onIndexedEntityUpdated(new String(unit.get().getKey()), new SegmentIndexInfoImpl(i));

                    context.currentTableContext().getTableIndex().onIndexedEntityUpdated(new String(unit.get().getKey()), segment);

                    i += unit.get().getUnitSize();
                }
            }
        } catch (IOException e) {
            throw new DatabaseException(e.getMessage());
        }
    }
}
