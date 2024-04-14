This library help querying sql db using a GlobType.

Exemple of a simple query

```
        sqlConnection.createTable(DummyWithDateTime.TYPE);
        sqlConnection.getCreateBuilder(DummyWithDateTime.TYPE)
                .set(DummyWithDateTime.uuid, "AAAAA")
                .set(DummyWithDateTime.date, LocalDate.of(2022, 10, 3))
                .set(DummyWithDateTime.created, ZonedDateTime.of(LocalDate.of(2022, 10, 3),
                        LocalTime.of(12, 0, 0), ZoneId.systemDefault()))
                .getRequest()
                .run();
        sqlConnection.commit();
        
        
...

        SelectQuery query =
                sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.and(
                                Constraints.equal(DummyObject.ID, 1),
                                Constraints.and(), null, null))
                        .select(DummyObject.ID, idAccessor)
                        .select(DummyObject.NAME, nameAccessor)
                        .select(DummyObject.PRESENT)
                        .select(DummyObject.COUNT)
                        .select(DummyObject.VALUE).getQuery();

        DbStream requestStream = query.execute();
        assertTrue(requestStream.next());
        assertEquals(1, idAccessor.get().getValue(0));
        assertEquals("hello", nameAccessor.get().getString());
        assertNull(requestStream.getAccessor(DummyObject.COUNT).getObjectValue());

```


