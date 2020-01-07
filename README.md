# metabase-kylin-driver

metabase 的 kylin jdbc驱动

由于不懂clojure，数据库连接信息和连接池在代码里面写死了，因为metabase框架下用的c3p0连接池有问题，
一直报错，所以就自己创建了个连接池，解决了连接过多问题

使用方法：

1. 将代码拷贝到metabase项目的`modules/drivers/`目录下，并重命名kylin

2. 修改代码中的连接信息

> 暂时只支持一个写死的kylin项目，或者自己修改代码

3. 在metabase项目project.clj下添加依赖

```clojure
[org.apache.kylin/kylin-jdbc "2.6.4"]
```

4. 在metabase项目目录下执行，编译出jar包

```bash
bin/build-driver.sh kylin
```

5. 部署的时候，在metabase.jar包目录下，建立一个目录plugins，将驱动包拷进去
