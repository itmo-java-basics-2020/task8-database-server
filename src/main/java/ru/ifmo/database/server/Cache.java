package ru.ifmo.database.server;

public interface Cache {
    String get(String key);

    void set(String key, String value);
}
