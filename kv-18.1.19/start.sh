nohup java -Doracle.kv.security=/home/ankh/nosql/kv-18.1.19/kvroot/security/user.security -jar lib/kvstore.jar start -host localhost -port 5000 -root kvroot
java -Doracle.kv.security=/home/ankh/nosql/kv-18.1.19/kvroot/security/user.security -jar lib/kvstore.jar status -host localhost -port 5000 -root kvroot
