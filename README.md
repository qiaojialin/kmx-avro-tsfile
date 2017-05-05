# kmx-avro-tsfile

本项目用来将Avro转化为TsFile


#schema转化
其中avro中的iskey=true字段会按以下方式拼成tsfile中的delta_object字段。各个key之间以 **+** 连接，key和value之间用 **:** 分隔。

示例：key1:value1+key2:value2


#示例：cn.edu.thu.tsfile.Example
cn.edu.thu.tsfile.Example是示例程序。主要功能为将avro的schema转为tsfile的schema，并且将avro的GenericRecord转化为TSRecord并写入文件中。


#配置文件：xxx.yaml
AvroConverter可以传入一个配置文件，配置文件主要配置各个sensor的编码方式，其中sensor名字应该是alias。如果没有配置文件，默认是RLE编码，适用int,long,float,double数据类型。
