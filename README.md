# sql-json
This project aims to create a library to interface interaction with json via SQL queries. The idea behind it 
is that SQL is a very well-know language for querying, a lot of people are familiar with the syntax, more than json path.
So this project tries to fill the gap providing a bridge between Json and SQL querying.

The idea is to have this library to be base for other projects, for example a jdbc-driver to query json, or UI libraries.

## Supported operations
At the moment  this library supports a limited set of SQL operations, which is as follows:

- Select a list of columns, from Json ```SELECT col1, col2 from jsonElement```
- Use nested elements in the JSON ```SELECT el.val from outerJsonElement.innerElement```
- Use where Clause: ```SELECT col from element where fieldA.innerField = 'test'```
  - Following filters currently implemented:
    - equals (=) and not equals (!=). Ex: ```where fieldA = fieldB``` or ```where fieldA != 'val'```
    - Numeric comparisons with: <, >, >=, <=. Ex: ```where fieldA > 10```
    - Numeric operations (+,-,*,/). Ex: ```where fieldA + fieldB = 10```
    - AND, OR, NOT. Ex: ```where fieldA = 1 AND NOT(fieldB = 10)```
    - IS NULL, IS NOT NULL. Ex: ```where fieldA is not null``` 
    - IN clause. Ex: ```where fieldA in ('a', 'b', 'c')```
    - Numeric between. Ex: ```where fieldA between 1 and 3```
    - Precedence using parenthesis. Ex: ```where (fieldA between 1 and 3) AND (fieldB = 1 or fieldB = 3)```
- Distinct in the field projections. Ex: ```select distinct fieldA from element```

## Library specific operations
Also in order to overcome some limitations while filtering Json and returning data, some specific language was created.

### Select the entire Json instead of columns
While querying, sometimes is desirable to return the whole JSON element instead of columns. To make this possible 
we allowed the use of _____"."_____ in the select projection. In this case a formatted Json structure is being returned instead 
of broken into columns.

Example: ```SELECT "." from element```

### Query on the JSON root element instead of a sub-element
Some JSON contains the list of elements directly as root, in order to allow querying into root, use table names as ".".
With that notation we will use the element as root for the query. 
For example:
```
[
  {
    "name": "Alabama",
    "abbreviation": "AL"
  },
  {
    "name": "Alaska",
    "abbreviation": "AK"
  }
]
```

Then you could query as: ```SELECT name from "."```

### matching into lists
Differently than databases, JSON structures are more complex. You might want to filter a field that is in fact
, in order to provide this capability two functions where created.


#### matchAny
Function matchAny was created to allow filtering in a list element when you want to bring the result, if any element of the list
matched your other side expression.
Example: 
```
{
  "levels" : [
    {
      "name": "level1",
      "elements" : [
        {
          "name": "Level1Element1",
          "order" : 1,
          "nodes": [
            {
              "value": "level1Element1Node1"
            },
            {
              "value": "level1Element1Node2"
            }
          ]
        },
        {
          "name": "Level1Element2",
          "order" : 2,
          "nodes": [
            {
              "value": "level1Element2Node1"
            },
            {
              "value": "level1Element2Node2"
            }
          ]
        }
      ]
    },
    {
      "name": "level2",
      "elements" : [
        {
          "name": "Level2Element1",
          "order" : 3,
          "nodes": [
            {
              "value": "level2Element1Node1"
            },
            {
              "value": "level2Element1Node2"
            }
          ]
        },
        {
          "name": "Level2Element2",
          "order" : 4,
          "nodes": [
            {
              "value": "level2Element2Node1"
            },
            {
              "value": "level2Element2Node2"
            }
          ]
        }
      ]
    }
  ]
}
```
Let's say you want to retrieve any level that contains the element name equals to, 'Level1Element1':
```select * from levels where matchAny(elements.name) = 'Level1Element1'```
  
This would bring level1, because no element in level2 has that element. 

#### matchAll
Function matchAll is similar of matchAny and has the same purpose, the difference in this one is that all the elements in 
that list has to match the criteria, otherwise the element would not be returned.
Let's analyze following query against same data from matchAny example:
```select * from levels where matchAll(elements.name) = 'Level1Element1'```

Using this query no element would be returned, because there is no element where the criteria matches all in the list.

## Using the library
the use of the library is very straight-forward.

First you need to import it, if you're using Maven you can do:
```
<dependency>
    <groupId>io.github.spartatech</groupId>
    <artifactId>sql-json</artifactId>
    <version>0.5</version>
</dependency>
```

Then you need to instantiate class SqlJson (This currently allows 3 types of JSON inputS (String, File, InputStream)).
Last you need to execute your query.

```
final SqlJson sqlj = new SqlJson(json);
final JsonResultSet results = sqlj.queryAsJSONObject("select fieldA from element");
```

This will process the query and return an object of __JsonResultSet__.
This object allows you to iterate over the resultSet and retrieve data. Example of use:
```
final JsonResultSet results = sqlj.queryAsJSONObject("select fieldA from element");
while(results.next()) {
  results.getString("name"));
  results.getInt("idade"));
}
```
## Configurations
Library provide some customizations this configuration can be set using class ``SqlJsonConfigurer`` and should be send 
during instantiation of ``SqlJson``.

```
final SqlJson sqlj = new SqlJson(json, SqlJsonConfigurer.instance().strictResultRowExistence());
```

The current options are:

``strictResultRowExistence()``
Enabling this will force result items in the projection selection to exist,
otherwise it fails the query.

## Next Steps
This library is still in early development process, there are many more operations intended to be added. 
Please feel free to request new features or report issues in the Issue section on Github.