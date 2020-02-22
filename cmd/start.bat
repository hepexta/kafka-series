@echo off
set DIR="d:\Work\kafka"
set OUTPUT=d:\Work\kafka\kafka_2.12-2.4.0
set ZKBAT=%OUTPUT%\bin\windows\zookeeper-server-start.bat
set KAFKA_BAT=%OUTPUT%\bin\windows\kafka-server-start.bat
set KAFKA_CONFIG1=%OUTPUT%\config\server.properties
set KAFKA_CONFIG2=%OUTPUT%\config\server2.properties
set ZKCONFIG=%OUTPUT%\config\zookeeper.properties

if "2" == "2" (
set STDOUT_REDIRECTED=yes

rd /s /q %OUTPUT%\data
rd /s /q %OUTPUT%\logs
mkdir %OUTPUT%\data
mkdir %OUTPUT%\logs

start cmd /k call %ZKBAT% %ZKCONFIG%
timeout /t 10

start cmd /k call %KAFKA_BAT% %KAFKA_CONFIG1%
timeout /t 10

start cmd /k call %KAFKA_BAT% %KAFKA_CONFIG2%
echo launched
exit /b %ERRORLEVEL%
)
echo ZKBAT
echo KAFKA_BAT
echo KAFKA_BAT
echo KAFKA_CONFIG1
echo KAFKA_CONFIG2
echo ZKCONFIG
