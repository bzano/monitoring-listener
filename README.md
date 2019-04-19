## Oozie kafka job listener

This module allows to send monitoring metrics into Kafka, this solution connects directly to Oozie by implementing Oozie JobEventListener. This has the advantage of direct instrumentation

### Jar preparation

After cloning the project locally, run

```
mvn clean package
```
This will generate a jar (monitoring-listener-0.0.1) in the target directory

### Installation

Put the jar into oozie lib directory

Add or update this properties these properties in oozie-site.xml file

```xml
<property>
    <name>oozie.services.ext</name>
    <value>
        ...
        org.apache.oozie.service.EventHandlerService,
    </value>
</property>

<property>
    <name>oozie.service.EventHandlerService.event.listeners</name>
    <value>org.apache.oozie.event.listener.monitoring.MonitoringListener</value>
</property>

<property>
    <name>oozie.job.listener.kafka.bootstrap.servers</name>
    <value>kafka:9092</value>
</property>

<property>
    <name>oozie.job.listener.kafka.topic</name>
    <value>tpc-oozie-monitoring</value>
</property>

```

#### Log4j configuration (optional)
You can also update the log4j configuration by editing the log4j.properties file

```
log4j.appender.oozieKafka=org.apache.log4j.rolling.RollingFileAppender
log4j.appender.oozieKafka.RollingPolicy=org.apache.oozie.util.OozieRollingPolicy
log4j.appender.oozieKafka.File=${oozie.log.dir}/oozie-kafka-listener.log
log4j.appender.oozieKafka.Append=true
log4j.appender.oozieKafka.layout=org.apache.log4j.PatternLayout
log4j.appender.oozieKafka.layout.ConversionPattern=%d{ISO8601} %5p %c{1}:%L - SERVER[${oozie.instance.id}] %m%n
log4j.appender.oozieKafka.RollingPolicy.FileNamePattern=${log4j.appender.oozieKafka.File}-%d{yyyy-MM-dd-HH}
log4j.appender.oozieKafka.RollingPolicy.MaxHistory=720

log4j.logger.org.apache.oozie.event.listener.monitoring=INFO, oozieKafka
```

Restart oozie
