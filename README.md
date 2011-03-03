### Project Description

<a class="externalLink" href="http://www.mono-project.com/Main_Page">Mono</a> compatible .NET/C# library with set of primitives to work with [table][1], [queue][1] and [block containers][1] with corresponding implementations for <a class="externalLink" href="http://aws.amazon.com/simpledb/">Amazon SimpleDB</a>, <a class="externalLink" href="http://aws.amazon.com/sqs/">SQS</a> and <a class="externalLink" href="http://aws.amazon.com/s3/">S3</a>. Additionally includes local machine (file system and <a class="externalLink" href="http://sqlite.org/">SQLite</a>) implementations to enable debugging and testing. Implemented in C# 3.0 programming language. Additionally includes [Amazon Web Services Shell (AwsSh)][2] command line utility for interactive access to Amazon SimpleDB, SQS and S3 services.

### Scalable Table

**Features**

*   Support of full Amazon SimpleDB data model (multi-valued attributes) via table reader and writers
*   Support for <a class="externalLink" href="http://docs.amazonwebservices.com/AmazonSimpleDB/latest/DeveloperGuide/index.html?SDB_API_BatchPutAttributes.html">batch put</a>
*   Support for <a class="externalLink" href="http://www.allthingsdistributed.com/2010/02/strong_consistency_simpledb.html">conditional updates and consistent read</a>
*   Paging support for large select result sets
*   Simplified mapping of SimpleDB item to BCL dictionary
*   Local file system implementation based on SQLite
*   XML based configuration
*   Cross-implementation unit tests with <a class="externalLink" href="http://xunit.codeplex.com/">xUnit</a>

**Planned**

*   Support for <a class="externalLink" href="http://docs.amazonwebservices.com/AmazonSimpleDB/latest/DeveloperGuide/index.html?SortingDataSelect.html">sorting</a>
*   Asynchronous implementation with <a class="externalLink" href="http://msdn.microsoft.com/en-us/library/aa719595(v=VS.71).aspx">BCL Begin/End pattern</a>
*   Option to use HTTPS for accessing Amazon Web Services

See [documentation][1].

### Scalable Queue

**Features**

*   Support of UTF8 string based messages
*   Support for BCL dictionary based messages (XML serialized)
*   Local file system implementation
*   XML based configuration 
*   Cross-implementation unit tests with <a class="externalLink" href="http://xunit.codeplex.com/">xUnit</a>

**Planned**

*   Support for .NET object serialization
*   Asynchronous implementation
*   Option to use HTTPS for accessing Amazon Web Services

See [documentation][1].

### Scalable Block

**Features**

*   Support for Amazon S3 block IO via streams over HTTP
*   Local file system implementation
*   XML based configuration 

**Planed**

*   Support for MD5 data validation
*   Support for Amazon S3 ACLs
*   Support for block metadata (content type, cache expiration etc.)
*   Adding xUnit tests
*   Asynchronous implementation
*   Option to use HTTPS for accessing Amazon Web Services

### Amazon Web Services Shell (AwsSh)

**Features**

*   Managing SimpleDB domains and items (list, create, delete, delete by prefix) 
*   Selecting SimpleDB items with select expression
*   Saving and loading SimpleDB items to and from XML file, additionally saving the result of select expression
*   Managing SQS queues (list, create, delete, delete by prefix) 
*   Managing S3 buckets and objects (list, create, delete, delete by prefix)
*   Uploading and dowloading S3 objects

See [documentation][2].

 [1]: /documentation?referringTitle=Home
 [2]: /wikipage?title=AwsShDocumentation&referringTitle=Home
