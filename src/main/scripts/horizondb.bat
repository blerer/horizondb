@REM Copyright 2014 Benjamin Lerer
@REM 
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM 
@REM  http://www.apache.org/licenses/LICENSE-2.0
@REM 
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM 

@echo off
if "%OS%" == "Windows_NT" SETLOCAL

pushd %~dp0..
if NOT DEFINED HORIZONDB_HOME set HORIZONDB_HOME=%CD%
popd

if NOT DEFINED HORIZONDB_MAIN set HORIZONDB_MAIN=io.horizondb.db.HorizonDbDaemon
if NOT DEFINED JAVA_HOME goto :err

set JAVA_OPTS=-Xms1G^
 -Xmx1G^
 -XX:+HeapDumpOnOutOfMemoryError^
 -XX:+UseParNewGC^
 -XX:+UseConcMarkSweepGC^
 -XX:+CMSParallelRemarkEnabled^
 -XX:SurvivorRatio=8^
 -XX:MaxTenuringThreshold=1^
 -XX:CMSInitiatingOccupancyFraction=75^
 -XX:+UseCMSInitiatingOccupancyOnly

set CLASSPATH="HORIZONDB_HOME%\conf"

for %%i in ("%HORIZONDB_HOME%\lib\*.jar") do call :append "%%i"
goto :runDaemon

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

:runDaemon
echo Starting HorizonDB
"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% "%HORIZONDB_MAIN%"
goto finally

:err
echo JAVA_HOME environment variable must be set
pause

:finally

ENDLOCAL