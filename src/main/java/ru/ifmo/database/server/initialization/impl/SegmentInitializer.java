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
            throw new DatabaseException("Segment must not be null");
        }

        Segment segment = new SegmentImpl(context.currentSegmentContext());
        context.currentTableContext().updateCurrentSegment(segment);

        File file = new File(context.currentSegmentContext().getSegmentPath().toString());

        int cursorIndex = 0;
        try (DatabaseInputStream in = new DatabaseInputStream(new FileInputStream(file))) {
            while (cursorIndex < file.length()) {
                Optional<DatabaseStoringUnit> data = in.readDbUnit(0);

                if (data.isEmpty()) {
                    throw new DatabaseException("File is empty");
                }

                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(
                        new String(data.get().getKey()),
                        new SegmentIndexInfoImpl(cursorIndex)
                );
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(
                        new String(data.get().getKey()),
                        segment
                );
                cursorIndex += data.get().getFileSize();
            }
        } catch (IOException ignored) {

        }
    }
}
