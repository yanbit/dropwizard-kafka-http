# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/sh -Eux

#  Trap non-normal exit signals: 1/HUP, 2/INT, 3/QUIT, 15/TERM, ERR
trap founderror 1 2 3 15 ERR

founderror()
{
        exit 1
}

exitscript()
{
        #remove lock file
        #rm $lockfile
        exit 0
}

apt-get -y update

apt-get install -y software-properties-common python-software-properties screen vim git wget
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

export JAVA_HOME=/usr

su vagrant -c "touch ~/.bashrc"
su vagrant -c "echo 'export JAVA_HOME=/usr' >> ~/.bashrc"

cd /tmp
wget https://archive.apache.org/dist/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz
mkdir -p /opt/apache
cd /opt/apache/
tar -xvf /tmp/apache-maven-3.1.1-bin.tar.gz
export PATH=/opt/apache/apache-maven-3.1.1/bin:$PATH
su vagrant -c "echo 'export PATH=/opt/apache/apache-maven-3.1.1/bin:$PATH' >> ~/.bashrc"

cd /vagrant
mvn clean
mvn package

/vagrant/vagrant/kafka.sh #install kafka

/opt/apache/kafka/bin/zookeeper-server-start.sh /opt/apache/kafka/config/zookeeper.properties 1>> /tmp/zk.log 2>> /tmp/zk.log &

