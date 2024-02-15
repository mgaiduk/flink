As you might've noticed, previous tutorials (running kinesis + dynamodb locally vs in aws) differ mainly in the parameters that we pass to the source and sink functions. For local run, we provide endpoint urls of aws daemon running locally, and some bogus api keys. For cloud run, these details are handled automatically by Managed Apache Flink service, so we don't need to do that. Some details, like table names and offset options, are needed in both cases.   

It is a good practice to store such details in a config. But the only interface we have for cluster communication is the `.jar` file; we cannot provide any extra files or cmd line arguments. Here is where gradle's flexibility finally comes in handy: we can specify a *build time command line argument*, choose a config file based on that, and put it in the final `.jar` file so it can be read at job startup as a dependency.   

## Modifying build script
First, we'll need to parse yaml configs, so let's add yaml parser lib as dependency:   
```Kotlin
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")
```
Next, add the following code to `build.gradle.kts`:   
```Kotlin
val selectConfig by tasks.registering(Copy::class) {
    val config = project.findProperty("config")?.toString() ?: "prod.yaml"

    from("src/main/resources/config")
    include("$config")
    rename("$config", "config.yaml")
    into("build/resources/main")
}

tasks.named("processResources") {
    dependsOn(selectConfig)
}
```
First, we define a function `selectConfig`, delegating property definition to `Copy` class using kotlin's "by" syntax. This class is provided by Gradle, and is the one responsible for copying files around during gradle build.    

We look for a property named `config`. This property can be provided at build time, by specifying `-P` option:   
```bash
./gradlew jar -Pconfig=prod.yaml
```
We use `prod.yaml` by default if no arg is specified. We look for a file with this name in `src/main/resources/config` directory; `src/main/resources` is the standard folder in JVM to store such files.   

We rename the file to `config.yaml` so that it is always accessible in code by the same name. Finally, we specify that `processResources` task - standard task in gradle for jvm, provided by `jvm` plugin, that is responsible for gathering resources for the jar file - depends on our newly registered `selectConfig` task, i.e., it will wait for its execution before starting.   

## Writing configs
Here is the `debug.yaml` config:   
```yaml
source:
  awsEndpoint: "http://localhost:4567"
  awsRegion: "us-east-1"
  awsAccessKeyId: "dummy"
  awsSecretAccessKey: "dummy"
  streamInitialPosition: "TRIM_HORIZON"
  datastreamName: "flink-test"
  kinesaliteTimestampFix: True
sink:
  awsRegion: "us-east-1"
  awsEndpoint: "http://localhost:8000"
  tableName: "flink-test"
```
And `prod.yaml`:   
```yaml
source:
  awsRegion: "us-east-1"
  streamInitialPosition: "TRIM_HORIZON"
  datastreamName: "flink-test"
sink:
  awsRegion: "us-east-1"
  tableName: "flink-test"
```
## Parsing configs in code
Put this in your `App.kt` file:   
```Kotlin
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.io.InputStream

data class SourceConfig(
    val awsEndpoint: String?,
    val awsRegion: String?,
    val awsAccessKeyId: String?,
    val awsSecretAccessKey: String?,
    val streamInitialPosition: String = "TRIM_HORIZON",
    val datastreamName: String,
    val kinesaliteTimestampFix: Boolean = false,
)

data class SinkConfig(
    val awsEndpoint: String?,
    val awsRegion: String?,
    val tableName: String,
)

data class AppConfig(
    val source: SourceConfig,
    val sink: SinkConfig,
)

fun loadConfig(): AppConfig {
    val yamlReader = ObjectMapper(YAMLFactory()).registerKotlinModule()
    val inputStream: InputStream = Thread.currentThread().contextClassLoader.getResourceAsStream("config.yaml")
        ?: throw IllegalArgumentException("file not found!")
    return yamlReader.readValue(inputStream, AppConfig::class.java)
}
```
We specify format by just defining some classes with field names corresponding to field names in the config. We load config data, looking for a file with a fixed name - `config.yaml`. Then, we use `jackson` yaml parsing library to parse it.
## Using config in flink code
I've decided to do a bit of refactoring, separating source and sink definitions into different functions for clarity:   
```Kotlin
fun initKinesisSource(config: SourceConfig): FlinkKinesisConsumer<String> {
    val properties = Properties()
    if (config.awsEndpoint != null) {
        properties[AWSConfigConstants.AWS_ENDPOINT] = config.awsEndpoint
    }
    if (config.awsRegion != null) {
        properties[AWSConfigConstants.AWS_REGION] = config.awsRegion
    }
    if (config.awsAccessKeyId != null) {
        properties[AWSConfigConstants.AWS_ACCESS_KEY_ID] = config.awsAccessKeyId
    }
    if (config.awsSecretAccessKey != null) {
        properties[AWSConfigConstants.AWS_SECRET_ACCESS_KEY] = config.awsSecretAccessKey
    }
    properties[ConsumerConfigConstants.STREAM_INITIAL_POSITION] = config.streamInitialPosition
    val kinesisConsumer = FlinkKinesisConsumer(config.datastreamName, SimpleStringSchema(), properties)
    return kinesisConsumer
}

fun initDynamodbSink(config: SinkConfig): DynamoDbSink<AggregatedEvent> {
    val properties = Properties()
    if (config.awsEndpoint != null) {
        properties[AWSConfigConstants.AWS_ENDPOINT] = config.awsEndpoint
    }
    if (config.awsRegion != null) {
        properties[AWSConfigConstants.AWS_REGION] = config.awsRegion
    }

    val dynamoDbSink = DynamoDbSink.builder<AggregatedEvent>()
        .setTableName(config.tableName)
        .setElementConverter(CustomElementConverter())
        .setMaxBatchSize(20)
        .setOverwriteByPartitionKeys(listOf("key"))
        .setDynamoDbProperties(properties)
        .build()
    return dynamoDbSink
}
```
Now, we can use them in our main script:   
```Kotlin
fun runJob() {
    val config = loadConfig()

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val kinesisConsumer = initKinesisSource(config.source)
    val kinesisSource = env.addSource(kinesisConsumer)
    // checkpoint every minute
    // env.enableCheckpointing(60000);
    val dynamoDbSink = initDynamodbSink(config.sink)

    defineWorkflow(kinesisSource, config) { workflow -> 
            workflow.sinkTo(dynamoDbSink) 
        }
    env.execute()
}
```
Finally, we also use our `kinesaliteTimestampFix` flag to decide if we need to fix timestamp (relevant when running local kinesis, kinesalite) or not. We do it in the workflow definition itself:   
```Kotlin
fun defineWorkflow(
    source: DataStream<String>,
    config: AppConfig,
    sinkApplier: (stream: DataStream<AggregatedEvent>) -> Unit
) {
    var watermarkStrategy = WatermarkStrategy
        .forMonotonousTimestamps<Event>()
    if (config.source.kinesaliteTimestampFix) {
        watermarkStrategy = watermarkStrategy.withTimestampAssigner { _event: Event, timestamp: Long -> timestamp * 1000
        }
    } else {
        watermarkStrategy = watermarkStrategy.withTimestampAssigner { _event: Event, timestamp: Long -> timestamp
        }
    }
    ...
}
```