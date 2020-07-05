[![Build Status](https://travis-ci.org/zakgof/velvetdb.svg?branch=master)](https://travis-ci.org/zakgof/velvetdb)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.zakgof/velvetdb-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.zakgof/velvetdb-core)

# Overview

Velvetdb is a high-level API for NoSQL storage perfectly fitting for small websites, desktop and mobile applications.
With zero configuration and simple API you'll need just 5 minutes to start using velvetdb.
Velvetdb abstracts out the underlying database concepts like columns or tables and provides essentially storage for you Java classes.

Velvetdb is implemented on top the following backends:

  - [xodus](https://github.com/JetBrains/xodus) - extremely fast embedded database
  - [dynamodb](https://aws.amazon.com/dynamodb/) - cloud database


Velvetdb's design is based on these principles making it distinct:
- **Least intrusive.**  
 We won't force developers to design their data model *around* the persistence framework. No need to extend framework's base classes or implement framework's interfaces. Persist your POJOs with no or minimal limitations and no or minimal framework dependencies. You can even use 3rd party model classes if needed.
- **Decoupled where possible.**  
 In our API we try to keep all concepts separated as much as possible. Entities do not know anything about their Relationships. Queries are objects that can be pre-created and separated from application's business logics.

[More on philosophy on Wiki...](https://github.com/zakgof/velvetdb/wiki/VelvetDB-vs-traditional-approach)

## Data model fundamentals
- **Data is a Graph**. Graph is a natural structure for most real data. Nodes are entities, edges represent relationships.
- **Entity** is a POJO. Basic CRUD operations are applicable.
- An entity should either have a **Primary key** or framework will assign it an autogenerated key. Range queries are supported for sortable primary keys.
- **Secondary indexes** are supported with range queries applicable.
- One-to-one, one-to-many and many-to-many **relationships** are supported. Range queries on linked entities are supported.

## Main features

- Pure Java 8. No xmls or other config files needed
- Transactional (on backends that support transactions)
- Pluggable serialization library ([kryo](https://github.com/EsotericSoftware/kryo) is the default; [elsa](https://github.com/jankotek/elsa) also supported)
- Android support (on xodus), min SDK version is 19
- Join queries
- Automatic schema migration support


# Android notes

Velvetdb uses Java 8 features and Java 8 APIs. In order to use them on older Android platforms, use Android Gradle plugin 4.0.0+ with [system lib desugaring](https://developer.android.com/studio/write/java8-support#library-desugaring).
For ProGuard/R8 config refer to [consumer-rules.pro](https://github.com/zakgof/velvetdb/tree/master/velvetdb-core/consumer-rules.pro). 

# Getting Started

## Setup

velvetdb is on Maven Central

Select the artifact matching your choice of backend: velvetdb-xodus, velvetdb-mapdb or velvetdb-dynamodb

```xml
<dependency>
    <groupId>com.github.zakgof</groupId>
    <artifactId>velvetdb-xodus</artifactId>
    <version>0.9.0</version>
</dependency>
```
or, using Gradle:
```groovy
compile 'com.github.zakgof:velvetdb-xodus:0.9.0'
```

For Android version:

```groovy
compile 'com.github.zakgof:velvetdb-xodus-android:0.3.3'    
```

## Define entities using annotations
```java
   public class Book {
        @Key
        private String isbn;
        private String title;
        private int year;
    }

    @Keyless // key will be autogenerated
    public class Author {
        private String firstName;
        private String lastName;
    }
    
    // define entities
    IEntityDef<String, Book> BOOK = Entities.create(Book.class);
    IKeylessEntityDef<Author> AUTHOR = Entities.keyless(Author.class);

    // define one-to-many relationship
    IMultiLink<Long, Author, String, Book> AUTHOR_BOOKS = Links.multi(AUTHOR, BOOKS); 
```
## Working with velvetdb
```java
    // Say you have some POJOs to store
    Book book = new Book...
    Author author = new Author...

    // Use /home/velvuser/db as embedded database folder
    IVelvetEnvironment env = VelvetFactory.open("velvetdb://xodus/home/velvuser/db"));
    
    // Run in a transaction
    env.execute(velvet -> {
    	   AUTHOR.put(velvet, author);
    });
       
    // We'll skip wrapping into transaction in the subsequent examples.
    
    // Store book and connect it with its author
    BOOK.put(velvet, book);
    AUTHOR_BOOKS.connect(velvet, author, book);
    
    // Get all books
    List<Book> allBooks = BOOK.batchGetAll(velvet);
    
    // Or fetch by a key
    Book book = BOOK.get(velvet, isbn);
    
    // Now get all books of a specific author:
    List<Book> authorsBooks = AUTHOR_BOOKS.get(velvet, author);
```

[More on Wiki...](https://github.com/zakgof/velvetdb/wiki)
