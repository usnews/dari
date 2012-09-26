Dari is an object persistence library. Read on for a quick tour, or see the
full [API documentation](http://hudson.psddev.com/job/dari-master/ws/db/target/apidocs/index.html).

Configuration
=============

Ensure that a reference to the [PSD Maven repository](http://psddev.com/maven)
exists in your project's [Apache Maven](http://maven.apache.org/) POM file:

    <repository>
        <id>psddev</id>
        <url>http://psddev.com/maven</url>
        <snapshots>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
   
And add the following dependency:

    <dependency>
        <groupId>com.psddev.dari</groupId>
        <artifactId>dari-db</artifactId>
        <version>1.5.0-SNAPSHOT</version>
    </dependency>

If you're creating a web application, you should add the following dependency
as well:

    <dependency>
        <groupId>com.psddev.dari</groupId>
        <artifactId>dari-servlet</artifactId>
        <version>1.5.0-SNAPSHOT</version>
    </dependency>
    <dependency>
        <groupId>com.psddev.dari</groupId>
        <artifactId>dari-web-servlet</artifactId>
        <version>1.5.0-SNAPSHOT</version>
    </dependency>

Along with the following elements in your `web.xml`:

    <filter>
        <filter-name>DebugFilter</filter-name>
        <filter-class>com.psddev.dari.servlet.DebugFilter</filter-class>
    </filter>
    <filter-mapping>
        <filter-name>DebugFilter</filter-name>
        <url-pattern>/*</url-pattern>
    </filter-mapping>

Modeling
========

Java classes should look like (getters and setters omitted for brevity):

    import com.psddev.dari.db.Record;

    public class Article extends Record {
        private String headline;
        private Topic topic;
        private Author author;
        private ReferentialText body;
    }

    public class Author extends Record {
        private String name;
        private StorageItem photo;
        private String bio;
    }

    public class Topic extends Record {
        private String name;
        private List<Topic> subTopics;
    }

Writing to the database
=======================

To save a record:

    Article article = new Article();
    article.setHeadline("new headline");
    article.save();

To delete a record:

    Article article = Query.findById(Article.class, id);
    article.delete();

Reading from the database
=========================

Generally, you can fetch records from the database using the `Query` class:

    Query
        .from(Article.class)
        .where("author = ?", author)
        .sortAscending("headline")
        .select(0, 10);

For more common operations:

    Article article = Query.findById(Article.class, id);
    Author author = Query.findUnique(Author.class, "name", name);
