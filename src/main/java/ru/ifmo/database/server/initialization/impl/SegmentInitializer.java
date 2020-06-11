package ru.ifmo.database.server.initialization.impl;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.initialization.InitializationContext;
import ru.ifmo.database.server.initialization.Initializer;
import ru.ifmo.database.server.logic.Segment;
import ru.ifmo.database.server.logic.impl.DatabaseInputStream;
import ru.ifmo.database.server.logic.impl.DatabaseStoringUnit;
import ru.ifmo.database.server.logic.impl.SegmentImpl;

import java.io.*;
import java.util.LinkedList;

public class SegmentInitializer implements Initializer {

    @Override
    public void perform(InitializationContext context) {
        LinkedList<String> keys = new LinkedList<>();
//        try (InputStreamReader inputStreamReader = new FileReader(context.currentSegmentContext().getTableRootPath()
//                + "\\" + context.currentSegmentContext().getSegmentName())) {
//            int offset = 0;
//            while (true) {
//                int keyLen = inputStreamReader.read();
//                char[] word = new char[keyLen];
//                inputStreamReader.read(word, 0, keyLen);
//                String key = new String(word);
//                int valueLen = inputStreamReader.read();
//                word = new char[valueLen];
//                inputStreamReader.read(word, 0, valueLen);
//                context.currentSegmentContext().getIndex().updateSegmentMap(key, offset);
//                keys.add(key);
//                offset = offset + 2 + valueLen + keyLen;
//            }
//        } catch (IOException | NegativeArraySizeException e) {
//            //
//        }
        try (DatabaseInputStream in = new DatabaseInputStream(new FileInputStream(context.currentSegmentContext().getTableRootPath()
                + "\\" + context.currentSegmentContext().getSegmentName()))) {
            int offset = 0;
            while (true) {
                DatabaseStoringUnit dbStoringUnit =  in.readDbUnit(0);
                context.currentSegmentContext().getIndex().updateSegmentMap(new String(dbStoringUnit.getKey()), offset);
                keys.add(new String(dbStoringUnit.getKey()));
                offset = offset + 8 + dbStoringUnit.getKeySize() + dbStoringUnit.getValueSize();
            }
        } catch (IOException | NegativeArraySizeException | NullPointerException e) {
            //
        }
        Segment newSegment = SegmentImpl.initializeFromContext(context.currentSegmentContext());
        for (String key : keys) {
            context.currentTableContext().getTableIndex().updateSegment(key, newSegment);
        }
        context.currentTableContext().addSegment(newSegment.getName(), newSegment);
    }
}
