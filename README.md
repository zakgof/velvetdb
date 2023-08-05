[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.zakgof/velvetdb-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.zakgof/velvetdb-core)

# Overview

Velvetdb is a Java persistent framework perfectly fitting for microservices, small websites, desktop and mobile applications.
With zero configuration and simple API you'll need just 5 minutes to start using velvetdb.
Velvetdb is Java-centric: you don't have to deal with tables or columns, it's essentially a storage for you Java classes.

Backed by xodus and kryo, velvetdb provides ultra-high performance for read and write operations and stored data in a compact format.

Velvetdb's design is based on these principles making it distinct:
- **Least intrusive.**  
 We won't force developers to design their data model *around* the persistence framework. No need to extend framework's base classes or implement framework's interfaces. Persist your POJOs with no or minimal limitations and no or minimal framework dependencies. You can even use 3rd party model classes if needed.
- **Decoupled where possible.**  
 In our API we try to keep all concepts separated as much as possible. Entities do not know anything about their Relationships. Queries are objects that can be pre-created and separated from application's business logics.

## Main features

- Pure Java 11. No xmls or other config files needed
- Transactional (on backends that support transactions)
- Android support

# Getting Started

## Setup

velvetdb is on Maven Central

Select the artifact matching your choice of backend: velvetdb-xodus, velvetdb-mapdb or velvetdb-dynamodb

```xml
<dependency>
    <groupId>com.github.zakgof</groupId>
    <artifactId>velvetdb-xodus</artifactId>
    <version>0.10.2</version>
</dependency>
<dependency>
    <groupId>com.github.zakgof</groupId>
    <artifactId>velvetdb-core</artifactId>
    <version>0.10.2</version>
</dependency>
<dependency>
    <groupId>com.github.zakgof</groupId>
    <artifactId>velvetdb-serializer-kryo</artifactId>
    <version>0.10.2</version>
</dependency>
```
or, using Gradle:
```groovy
implementation 'com.github.zakgof:velvetdb-xodus:0.10.2'
implementation 'com.github.zakgof:velvetdb-core:0.10.2'
implementation 'com.github.zakgof:velvetdb-serializer-kryo:0.10.2'
```

## Initializing a velvetdb environment
```java
    IVelvetEnvironment velvetEnv = VelvetFactory.create("xodus", "~/velvetdemo/");
```

## Define entities using annotations
```java
   // your class
   public class Book {
        @Key
        private String isbn;
        private String title;
        private int year;
    }
    
    // define an entity
    IEntityDef<String, Book> BOOK = Entities.create(Book.class);
     
```
## Working with velvetdb
```java
    // Say you have some POJOs to store
    Book book = new Book...
            
    // Store a book
    bookEntity.put()
        .value(book)
        .execute(velvetEnv);

    // Get a book by ISBN (primary index lookup)
    Book book = bookEntity.get()
        .key(isbn)
        .execute(velvetEnv);

    // Get all the books released in 2000 or later (secondary index query)
    List<Book> booksAfter2000 = bookEntity.index("year")
        .query()
        .gte(2000)
        .get()
        .asValueList()
        .execute(velvetEnv);

    // Delete a book by ISBN
    bookEntity.delete()
        .key(isbn)
        .execute(velvetEnv);
```

[More on Wiki...](https://github.com/zakgof/velvetdb/wiki)
