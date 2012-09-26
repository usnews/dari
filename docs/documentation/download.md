---
layout: basic
title: Download
section: download
---

## Download

The current release of Dari is 2.0-SNAPSHOT. Download the source code
from [Github](#source-code) or include it in your maven project.

### License

Dari is released under the [BSD license](license.html).

### Maven

PERFECT SENSE digital hosts a public maven repository with the latest
versions of Dari.  Add the following to your pom.xml:

    <repositories>
        <repository>
            <id>psddev</id>
            <url>http://public.psddev.com/maven/</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>com.psddev</groupId>
            <artifactId>dari-db</artifactId>
            <version>2.0-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>com.psddev</groupId>
            <artifactId>dari-util</artifactId>
            <version>2.0-SNAPSHOT</version>
        </dependency>
    </dependencies>

### Source Code

The Dari source code is hosted on
[Github](http://github.com/psddev/dari). Use the following Git command to download it:

    git clone https://github.com/psddev/dari.git


