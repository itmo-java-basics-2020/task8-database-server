package ru.ifmo.database.server.cache;


import java.util.Hashtable;

public class DatabaseCache implements Cache {
    private Hashtable<String, String> table = new Hashtable();

    public DatabaseCache() {

    }

    @Override
    public String get(String key) {
        return table.get(key);

    }

    @Override
    public void set(String key, String value) {
        table.put(key, value);
    }
}
