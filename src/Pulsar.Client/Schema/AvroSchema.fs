namespace Pulsar.Client.Schema

open System.Text
open Avro
open Avro.Generic
open Avro.IO
open Avro.Reflect
open System.IO
open Avro.Specific
open Pulsar.Client.Api
open AvroSchemaGenerator
open Pulsar.Client.Common

type internal AvroSchema<'T> private (schema: Schema, avroReader: DatumReader<'T>, avroWriter: DatumWriter<'T>) =
    inherit ISchema<'T>()
    let parameterIsClass =  typeof<'T>.IsClass
    
    new () =
         let tpe = typeof<'T>
         if typeof<ISpecificRecord>.IsAssignableFrom(tpe) then
            let avroSchema = downcast tpe.GetField("_SCHEMA").GetValue(null) :Schema
            let avroWriter = SpecificDatumWriter<'T>(avroSchema)
            // note afaik for schema evolutions, the writerSchema (first param) should come from the topic
            // (i.e the schema the value was written in) 
            let avroReader = SpecificDatumReader<'T>(avroSchema, avroSchema)
            AvroSchema(avroSchema, avroReader, avroWriter)
         else
            let schemaString = tpe.GetSchema()
            AvroSchema(schemaString)

    new (schemaString) =
        let avroSchema = Schema.Parse(schemaString)
        let avroWriter = ReflectWriter<'T>(avroSchema)
        let avroReader = ReflectReader<'T>(avroSchema, avroSchema)
        AvroSchema(avroSchema, avroReader, avroWriter)
        
    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.AVRO
        Schema = schema.ToString() |> Encoding.UTF8.GetBytes
        Properties = Map.empty
    }
    override this.SupportSchemaVersioning = true
    override this.Encode value =
        if parameterIsClass && (isNull <| box value) then
            raise <| SchemaSerializationException "Need Non-Null content value"
        use stream = MemoryStreamManager.GetStream()
        avroWriter.Write(value, BinaryEncoder(stream))
        stream.ToArray()
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        avroReader.Read(Unchecked.defaultof<'T>, BinaryDecoder(stream))        
    override this.GetSpecificSchema stringSchema =
        AvroSchema(stringSchema) :> ISchema<'T>
        
type internal GenericAvroSchema(topicSchema: TopicSchema) =
    inherit ISchema<GenericRecord>()    
    let stringSchema = topicSchema.SchemaInfo.Schema |> Encoding.UTF8.GetString
    let avroSchema = Schema.Parse(stringSchema) :?> RecordSchema
    let avroReader = GenericDatumReader<Avro.Generic.GenericRecord>(avroSchema, avroSchema)
    let schemaFields = avroSchema.Fields

    override this.SchemaInfo = {
        Name = ""
        Type = SchemaType.AVRO
        Schema = topicSchema.SchemaInfo.Schema
        Properties = Map.empty
    }
    override this.Encode _ = raise <| SchemaSerializationException "GenericAvroSchema is for consuming only!"
    override this.Decode bytes =
        use stream = new MemoryStream(bytes)
        let record = avroReader.Read(null, BinaryDecoder(stream))        
        let fields =
            schemaFields
            |> Seq.map (fun sf -> { Name = sf.Name; Value = record.[sf.Name]; Index = sf.Pos })
            |> Seq.toArray
        let scemaVersionBytes =
            topicSchema.SchemaVersion
            |> Option.map (fun (SchemaVersion bytes) -> bytes)
            |> Option.toObj
        GenericRecord(scemaVersionBytes, fields)