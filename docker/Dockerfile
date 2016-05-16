FROM java:8

MAINTAINER support@elodina.net

RUN apt-get -y update
RUN apt-get install -y lsb-release

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF
RUN export DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]') && export CODENAME=$(lsb_release -cs) && echo "deb http://repos.mesosphere.com/$DISTRO $CODENAME main" | tee /etc/apt/sources.list.d/mesosphere.list
RUN apt-get -y update
RUN apt-get -y install mesos=0.28*

RUN service zookeeper stop

RUN apt-get -y install wget git

RUN wget http://apache.ip-connect.vn.ua/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
RUN tar xvf apache-maven-3.3.9-bin.tar.gz

RUN wget https://raw.github.com/Netflix/exhibitor/master/exhibitor-standalone/src/main/resources/buildscripts/standalone/maven/pom.xml
RUN apache-maven*/bin/mvn clean package
RUN cp target/exhibitor-*.jar .

RUN wget http://cloudgrid.s3.amazonaws.com/bin/zookeeper-3.4.6.tar.gz
RUN tar -xf zookeeper-3.4.6.tar.gz

RUN git clone https://github.com/elodina/exhibitor-mesos-framework.git
RUN cd exhibitor-mesos-framework && ./gradlew jar
RUN mv exhibitor-mesos-framework/exhibitor-mesos-*.jar ./exhibitor-mesos.jar

ADD ./start-executor.sh /tmp/start-executor.sh

ENTRYPOINT ["/tmp/start-executor.sh"]
