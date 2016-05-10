FROM centos:7

MAINTAINER elodina

RUN yum -y update
RUN yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel wget

RUN wget http://apache.ip-connect.vn.ua/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
RUN tar xvf apache-maven-3.3.9-bin.tar.gz
RUN mv apache-maven-3.3.9 /usr/local/apache-maven

RUN wget https://raw.github.com/Netflix/exhibitor/master/exhibitor-standalone/src/main/resources/buildscripts/standalone/maven/pom.xml
RUN /usr/local/apache-maven/bin/mvn clean package
RUN cp target/exhibitor-*.jar .
RUN mkdir -p /opt/exhibitor
RUN mv exhibitor-*.jar /opt/exhibitor/exhibitor.jar

RUN wget http://cloudgrid.s3.amazonaws.com/bin/zookeeper-3.4.6.tar.gz
RUN tar -xf zookeeper-3.4.6.tar.gz
RUN mkdir -p /opt/zookeeper
RUN mv zookeeper-3.4.6 /opt/zookeeper

ENTRYPOINT ["java", "-jar", "/opt/exhibitor/exhibitor.jar"]