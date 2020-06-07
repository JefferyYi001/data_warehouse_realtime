#!/bin/bash
project_home=/home/atguigu/gmall_1128
jar_path=gmall-logger-0.0.1-SNAPSHOT.jar
case $1 in
"start")
    echo "===== 在 hadoop103 启动 Nginx ====="
    sudo /usr/local/webserver/nginx/sbin/nginx
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "===== 在 $host 启动日志服务器 ====="
        ssh $host "source /etc/profile; nohup java -jar $project_home/$jar_path 1>$project_home/log.log 2>$project_home/error.log &"
    done
    ;;

"stop")
    echo "===== 在 hadoop103 停止 Nginx ====="
    sudo /usr/local/webserver/nginx/sbin/nginx -s stop
    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "===== 在 $host 停止日志服务器 ====="
#        ssh $host "source /etc/profile; ps -ef | awk '/gmall-logger/ && !/awk/{print \$2}' | xargs kill -9"
        ssh $host "source /etc/profile; pkill -f $jar_path"
    done
    ;;

*)
    echo "脚本正确使用方式为："
    echo "start 启动 Nginx 和日志服务器"
    echo "stop 停止 Nginx 和日志服务器"
;;
esac