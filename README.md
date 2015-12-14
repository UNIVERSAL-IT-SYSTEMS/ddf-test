# ddf-test
Test suite for DDF

### Running test suite

1. Change the engine in [global] section and queries for your engine in [your engine] section in ddf-conf/ddf_spec.ini. 
Add a ddf.ini file for your engine. There are two ways to add the ddf-on-x jars:

  * Add ddf-on-x implementation jar to `lib` directory. Make sure that there are no duplicate jars present. Do not 
 include scalatest and junit jars . Do not remove the asm-all-4.0.jar from the lib directory as it
contains some essential classes required for the scalatest to generate a html report.

  * Execute the shell script which takes the location of a directory which has the ddf-on-x jars. Do not include 
 scalatest and junit jars in this directory. 

2. Running the tests in sbt console when the lib directory contains ddf-on-x jars.

```
$ sbt

//to run all the tests
sbt> test
```

3. Running the tests using the shell script.

```
$ bash bin/DDFTestRunner.sh

//Enter the required jar files and java Options in the command prompt.

Hello, welcome to ddf-test. This script will ask you choose an engine and run the tests in a GUI.

Choose your java options for tests (for spark: -Dhive.metastore.warehouse.dir=/tmp/hive/warehouse )

Enter your java options and press [ENTER]:

Enter your ddf-on-x jar's destination and press [ENTER]:

```

To open the Html Reports of the tests generated, open index.html from target/test-reports directory in a web browser.
Clicking on the DDFSpec suite, will show the test reports.

