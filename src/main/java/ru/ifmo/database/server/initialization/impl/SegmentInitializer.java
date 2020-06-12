package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.index.impl.SegmentIndexInfoImpl;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.initialization.SegmentInitializationContext;
import ru.ifmo.database.server.initialization.TableInitializationContext;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.SegmentOperationResult;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.SegmentImpl;
import ru.ifmo.database.server.logic.impl.SegmentReadResult;

import java.io.FileInputStream;
import java.io.IOException;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        TableInitializationContext tableContext = context.currentTableContext();
        SegmentInitializationContext segmentContext = context.currentSegmentContext();

        if (segmentContext == null) {
            throw new DatabaseException("Segment context  equals zero");
        }
        Segment segment = SegmentImpl.initializeFromContext(segmentContext);
        tableContext.addSegment(segment);

        DatabaseInputStream in;
        try {
            in = new DatabaseInputStream(new FileInputStream(segmentContext.getSegmentPath().toString()));
        } catch (IOException e) {
            throw new DatabaseException(e);
        }

        SegmentReadResult previousPart = tableContext.getPrevPart();
        int offset = 0;
        if (previousPart != null) {
            SegmentReadResult endingPart;
            try {
                endingPart = in.readDbUnit(previousPart);
            } catch (IOException e) {
                throw new DatabaseException(e);
            }

            SegmentReadResult resultDbUnit = SegmentReadResult.merge(previousPart, endingPart);
            offset = endingPart.getReadLength();
            if (resultDbUnit.getStatus() != SegmentOperationResult.OperationStatus.SUCCESS) {
                tableContext.setPrevPart(resultDbUnit);
                return;
            }
            InitializationContext initializationContext = tableContext.getInitializationContexts().get(tableContext.getPrevIndex());
            String previousResultKey = new String(resultDbUnit.getUnit().get().getKey());

            initializationContext.currentSegmentContext().getIndex().onIndexedEntityUpdated(previousResultKey,
                    new SegmentIndexInfoImpl(tableContext.getPrevOffset())); // segmentIndexing for the last unit in previous segment

            initializationContext.currentTableContext().getTableIndex().onIndexedEntityUpdated(previousResultKey,
                    initializationContext.currentTableContext().getSegment(previousResultKey)); // tableIndexing for the last unit in previous segment
        }
        while (true) {
            SegmentReadResult result;
            try {
                result = in.readDbUnit();
            } catch (IOException e) {
                throw new DatabaseException(e);
            }
            if (result.getStatus() == SegmentOperationResult.OperationStatus.SUCCESS) {
                String resultKey = new String(result.getUnit().get().getKey());
                segmentContext.getIndex().onIndexedEntityUpdated(resultKey,
                        new SegmentIndexInfoImpl(offset));
                tableContext.getTableIndex().onIndexedEntityUpdated(resultKey, segment);
                offset += result.getReadLength();
            } else {
                tableContext.setPrevPart(result);
                tableContext.setPrevIndex(tableContext.getCurrentIndex());
                tableContext.setPrevOffset(offset);
                break;
            }
        }

    }
}
