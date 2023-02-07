FROM mysql:8

LABEL maintainer="Debezium Community & Tugdual Grall"

ENV MYSQL_ROOT_PASSWORD=Kalangos
ENV MYSQL_USER=mysqluser
ENV MYSQL_PASSWORD=mysqlpw

EXPOSE 33060

COPY mysql.cnf /etc/mysql/conf.d/
COPY import-data.sql /docker-entrypoint-initdb.d/


