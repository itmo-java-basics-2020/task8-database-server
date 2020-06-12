package ru.ifmo.database.server.logic;

import ru.ifmo.database.server.exception.DatabaseException;
import ru.ifmo.database.server.logic.impl.SegmentReadResult;
import ru.ifmo.database.server.logic.impl.SegmentWriteResult;

import java.io.IOException;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public interface Segment {

    String getName();

    // todo sukhoa in future may return something like SegmentWriteResult .. with report and error details?
    // for new returns false if cannot allocate requested capacity
    // exception is questionable
    SegmentWriteResult write(String objectKey, String objectValue);

    /**
     * @param objectKey
     * @param objectValue
     * @param notPrintedLength - length of not printed part of DbUnit in bytes
     * @return SegmentWriteResult
     */
    SegmentWriteResult write(String objectKey, String objectValue, int notPrintedLength);

    SegmentReadResult read(String objectKey) throws IOException;

    /**
     *
     * @param objectKey
     * @param previousPart - part of DbUnit that was read in previous segment
     * @return
     * @throws IOException
     */
    SegmentReadResult read(String objectKey, SegmentReadResult previousPart) throws IOException, DatabaseException;

    boolean isReadOnly();
}