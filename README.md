
# [MyCAT](http://mycat.io/)
[![GitHub issues](https://img.shields.io/github/issues/MyCATApache/Mycat-Server.svg)](https://github.com/MyCATApache/Mycat-Server/issues)
[![GitHub forks](https://img.shields.io/github/forks/MyCATApache/Mycat-Server.svg)](https://github.com/MyCATApache/Mycat-Server/network)
[![GitHub stars](https://img.shields.io/github/stars/MyCATApache/Mycat-Server.svg)](https://github.com/MyCATApache/Mycat-Server/stargazers)
[![MyCAT](https://img.shields.io/badge/MyCAT-%E2%9D%A4%EF%B8%8F-%23ff69b4.svg)](http://mycat.io/)

基于 MyCAT 1.6 的改进

## Features

增加ZooKeeper全局序列号生产器类：IncrAtomicSequenceZKHandler
* JVM内访问同一个table序列的线程共享该table在本地缓存的序列区间 ，解决IncrSequenceZKHandler由于频繁跳号需要从ZK取序列空间引起的性能损失
* DistributedAtomicLong 实现跨JVM原子增加值，先尝试乐观锁，失败则用悲观锁 ，更好的性能
使用：
server.xml 文件 <property name="sequnceHandlerType">5</property>



