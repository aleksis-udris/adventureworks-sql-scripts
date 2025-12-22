# Schema Design & Specification

## Introduction

## Facts

Fact tables are definitive, they have 
all the nummeric values integral for analytics

In this project, we have 9 Fact tables,
each has 2 defining features:
- Atributes: Main nummeric values
- Connections to dimensions: Keys

In Clickhouse Fact Tables are defined using ``MergeTree()``

## Dimensions

Dimension Tables are constantly changing and updating,
Which means we need more fields for **Version Control**

We have 16 Dimensions, which describe the values
that Fact Tables have

- Static: Contain unchanging values
- SCD Type 1: Contain Values that could be overwritten
- SCD Type 2: Keep Old Values, but have additional 
  values to version rows

The defining features within Dimensions are:
- Describing Attributes (Names, Descriptions etc.)
- Versioning: Version and IsCurrent Values
- Access Key: Nummeric Values

In Clickhouse Dimension Tables are defined 
based on their type:
- ``ReplacingMergeTree(Version)``: For SCD Type 2 Tables
- ``MergeTree()``: For SCD Type 1 and Static Tables

## Aggregates



## Analytical Views