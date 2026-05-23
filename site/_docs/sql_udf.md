---
layout: docs
title: SQL User-Defined Functions
permalink: /docs/sql_udf.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

Calcite supports SQL User-Defined Functions (UDFs), allowing you to define
scalar functions using SQL expressions. This feature is similar to SQL UDFs
in PostgreSQL and Apache Spark.

* TOC
{:toc}

## Overview

SQL UDFs enable you to create reusable functions that encapsulate SQL expressions.
Unlike Java UDFs which require implementing a Java class, SQL UDFs allow you to
define functions using pure SQL syntax.

## Basic Syntax

### CREATE FUNCTION (SQL UDF)

```sql
CREATE [ OR REPLACE ] FUNCTION [ IF NOT EXISTS ] function_name
  ( [ parameter_definition [, ...] ] )
RETURNS return_type
RETURN function_body
```

Where:
- `function_name`: The name of the function (can be schema-qualified)
- `parameter_definition`: Parameter specification (see below)
- `return_type`: The data type returned by the function
- `function_body`: A SQL expression that computes the function result

### Parameter Definition

```sql
parameter_name data_type [ DEFAULT default_value ]
```

Where:
- `parameter_name`: The name of the parameter
- `data_type`: The SQL data type of the parameter
- `DEFAULT default_value` (optional): A default value for the parameter

**Note**: Currently, only IN parameters (input-only) are supported. OUT and INOUT 
parameter modes are planned for future versions.

## Examples

### Simple Function

A function that adds two numbers:

```sql
CREATE FUNCTION add(a INT, b INT)
RETURNS INT
RETURN a + b;
```

### Function with Default Parameter

A greeting function with a default parameter:

```sql
CREATE FUNCTION greet(name VARCHAR DEFAULT 'World')
RETURNS VARCHAR
RETURN 'Hello, ' || name;
```

Usage:
```sql
SELECT greet();           -- Returns 'Hello, World'
SELECT greet('Alice');    -- Returns 'Hello, Alice'
```

### Function with Multiple Parameters

A function that calculates the maximum of three numbers:

```sql
CREATE FUNCTION max_three(a INT, b INT, c INT)
RETURNS INT
RETURN CASE
  WHEN a >= b AND a >= c THEN a
  WHEN b >= a AND b >= c THEN b
  ELSE c
END;
```

### Complex Expression

A function that calculates a discount price:

```sql
CREATE FUNCTION apply_discount(price DECIMAL(10, 2), discount_percent INT)
RETURNS DECIMAL(10, 2)
RETURN ROUND(price * (1 - discount_percent / 100.0), 2);
```

### CREATE OR REPLACE

Update an existing function:

```sql
CREATE OR REPLACE FUNCTION calculate(x INT, y INT)
RETURNS INT
RETURN x * 2 + y;
```

### IF NOT EXISTS

Only create if the function doesn't exist:

```sql
CREATE FUNCTION IF NOT EXISTS double_value(x INT)
RETURNS INT
RETURN x * 2;
```

## Parameter Modes

Currently, Calcite SQL UDFs support only **IN** parameters (input-only):

```sql
CREATE FUNCTION add(a INT, b INT)
RETURNS INT
RETURN a + b;
```

All parameters are input parameters and are passed to the function.

### Using Return Values for Multiple Outputs

If you need to return multiple values, use a structured return type:

```sql
CREATE FUNCTION get_user_info(user_id INT)
RETURNS ROW(name VARCHAR, age INT, email VARCHAR)
RETURN (
  SELECT name, age, email FROM users WHERE id = user_id
);
```

### Future Enhancements

OUT and INOUT parameter modes are planned for future versions. These require 
careful semantic design to avoid side-effect and aliasing issues that would 
conflict with SQL's pure functional model.

## Type Compatibility

Function parameters and return types can be any SQL data type supported by Calcite,
including:
- Numeric types: INT, BIGINT, DECIMAL, FLOAT, DOUBLE
- String types: VARCHAR, CHAR, TEXT
- Date/Time types: DATE, TIME, TIMESTAMP
- Others: BOOLEAN, BINARY, etc.

## Limitations

Current limitations of SQL UDFs in Calcite:

- **Only IN parameters are supported** - OUT and INOUT modes are reserved for 
  future versions to maintain SQL's pure functional semantics
- Function body must be a single SQL expression (no BEGIN...END blocks)
- Recursive functions are not detected or prevented
- TABLE return type is not yet supported (scalar functions only)
- Parameters with DEFAULT values use only literal expressions

## Future Enhancements

Potential future improvements:

- **OUT and INOUT parameters** - Requires careful semantic design to handle 
  mutable reference semantics, aliasing, and execution order in SQL's 
  functional model
- **VARIADIC parameters** - Variable-length argument lists
- **TABLE return type** - Table-valued functions (currently scalar functions only)
- LANGUAGE clause to specify implementation language
- DETERMINISTIC/NOT DETERMINISTIC for optimization hints
- SECURITY DEFINER/INVOKER for permission control
- Complex expressions with multiple statements (BEGIN...END blocks)

## Related Topics

- [CREATE FUNCTION Reference](reference.html#createfunctionstatement)
- [Java UDFs]({{ site.apiRoot }}/org/apache/calcite/schema/ScalarFunction.html)
- [DDL Extensions](adapter.html#ddl-extensions)

## See Also

- PostgreSQL [CREATE FUNCTION](https://www.postgresql.org/docs/current/sql-createfunction.html)
- Apache Spark [SQL UDF](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-create-function.html)
