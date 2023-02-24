# AVRO codec (2.0.0)
## Description
Designed for decode AVRO raw messages to parsed messages and encode back.
It is based on [th2-codec](https://github.com/th2-net/th2-codec).
You can find additional information [here](https://github.com/th2-net/th2-codec/blob/master/README.md)

## Decoding

The codec decodes each raw message in the received batch.
The codec can work in two modes:
### Standard mode
Enabled when setting `avroMessageIdToDictionaryAlias` is filled.
Each raw message must contain AVRO header.
The first byte of the AVRO header is always 0(MAGIC BYTE).
It is followed by four bytes which identify the AVRO schema to be used for decoding.
```text
[0],[][][][], [][][][]][][]...[]
    schema id  data
```
### Schema resolution mode by session alias
Enabled when setting `sessionAliasToDictionaryAlias` is filled.
AVRO header of 5 bytes is missing. 
Schema resolves by session alias in message metadata.

All AVRO data types are supported, support conversion to AVRO standard logical types
Complex types are converted to:
```text
Record   -> Message
Map      -> Message
Array    -> List
Union    -> Object of resolve by schema
Enum     -> Value
```
Primitive types are converted to:
```text
Fixed    -> Value as Hex string
Bytes    -> Value as Hex string
Null     -> skip
other    -> Value 
```

**NOTE:  To the field name with a UNION value added is a prefix, because when converting to Value, the data type is lost**.

From the choice of the `enableIdPrefixEnumFields` setting, the prefix can be a `schema id`
```text
Id0-fieldName
```
or `Avro data type`
```text
Record-fieldName
```

After the prefix used a unique delimiter `-`
## Encoding
When encoding, all values are converted back, including logical type conversion. Before the data added `AVRO header`.

**NOTE: When encoding UNION values, a prefix is required for field names. This is described in the decoding section.**

## Settings
AVRO codec has the following parameters:

```yaml
enableIdPrefixEnumFields: false
avroMessageIdToDictionaryAlias: 
  '1': "${dictionary_link:avro-schema-1-dictionary}"
  '2': "${dictionary_link:avro-schema-2-dictionary}"
  '3': "${dictionary_link:avro-schema-3-dictionary}"
sessionAliasToDictionaryAlias:
  'sessionAlias1': "${dictionary_link:avro-schema-1-dictionary}"
  'sessionAlias2': "${dictionary_link:avro-schema-2-dictionary}"
  'sessionAlias3': "${dictionary_link:avro-schema-3-dictionary}"
```
**enableIdPrefixEnumFields** - prefix setting for UNION fields. If `false`, use prefix as `AVRO data type`(for example `Record-`, `Map-`), if `true` then use `schema id` prefix(for example `Id0-`, `Id3-`). The default value is `false`

**sessionAliasToDictionaryAlias** - matching `schema id` pairs with its `alias` available for loading in the pipelineCodecContext.

**avroMessageIdToDictionaryAlias** - matching `session alias` pairs with its `alias` available for loading in the pipelineCodecContext.

Only one of settings `sessionAliasToDictionaryAlias` or `avroMessageIdToDictionaryAlias` can be used

## Release notes

### 2.0.0
+ Migrated to book and page concept

### 1.0.0
+ Added data types described in [avro 1.10.2](https://avro.apache.org/docs/1.10.2/spec.html)

### 0.0.1