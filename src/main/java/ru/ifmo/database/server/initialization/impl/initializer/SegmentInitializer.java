package ru.ifmo.database.server.initialization.impl.initializer;

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

        Segment segment = new SegmentImpl(context.currentSegmentContext());
        context.currentTableContext().updateCurrentSegment(segment);

        File file = new File(context.currentSegmentContext().getSegmentPath().toString());

        // Filling segment indexes
        int cursorIndex = 0;
        try (DatabaseInputStream in = new DatabaseInputStream(new FileInputStream(file))) {
            while (cursorIndex < file.length()) {
                Optional<DatabaseStoringUnit> oData = in.readDbUnit(0);

                if (oData.isEmpty()) {
                    throw new DatabaseException("Some exceptions with files");
                }

                context.currentSegmentContext().getIndex().onIndexedEntityUpdated(
                        new String(oData.get().getKey()),
                        new SegmentIndexInfoImpl(cursorIndex)
                );
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(
                        new String(oData.get().getKey()),
                        segment
                );
                cursorIndex += oData.get().getSizeInFile();
            }
        } catch (IOException e) {
            // Nothing to do
        }
    }
}
