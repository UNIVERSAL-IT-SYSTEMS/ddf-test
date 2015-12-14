#!/bin/bash

# This is a program that starts a GUI to test a DDF-on-X implementation
echo ""
echo "Hello, welcome to ddf-test. This script will ask you choose an engine and run the tests in a GUI."

function Input(){

echo ""
echo "Choose your java options for tests (required for spark engine: -Dhive.metastore.warehouse.dir=/tmp/hive/warehouse )"
echo ""
echo -n "Enter your java options and press [ENTER]: "
read DDF_OPTIONS
echo ""
echo -n "Enter your ddf-on-x jar's destination and press [ENTER]: "
read DDF_JARS

}

function Run(){
SBT_OPTS="-Xms512M -Xmx1536M -Xss1M -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256M"
cd ..
rm -r "./metastore_db/"
rm -r "/tmp/hive/"


java $SBT_OPTS -jar "./bin/sbt-launch.jar" \
'set unmanagedJars in Test := ((file("'$DDF_JARS'") +++ file("./lib")) ** "*.jar").classpath' \
'set javaOptions in Test ++= Seq("'$DDF_OPTIONS'") ' \
'test'

}

function Main(){
Input
Run
}

Main
