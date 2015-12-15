# ddf-test
Test suite for DDF

### Running test suite

Change the engine in [global] section and queries for your engine in [your engine] section in ddf-conf/ddf_spec.ini.
 You can also create your own BaseSpec and then add its name to the baseSpec filed in [your engine] section. An 
 example baseSpec for AWS engine known as BaseSpecAWS is given. To use this baseSpecAWS add the AWS assembly jars and uncomment the lines corresponding to the same in baseSpecAWS.
  
Add a ddf.ini file for your engine. There are two ways to add the ddf-on-x jars:
  * Execute the shell script which takes the location of a directory which has the ddf-on-x jars.
  * Add ddf-on-x implementation jar to `lib` directory. 

  
### Running the tests using the shell script

```
$ ./bin/DDFTestRunner.sh

//Enter the required jar files and java Options in the command prompt.

Hello, welcome to ddf-test. This script will ask you choose an engine and run the tests.

Choose your java options for tests (for spark: -Dhive.metastore.warehouse.dir=/tmp/hive/warehouse )

Enter your java options or leave blank and press [ENTER]:

Enter your ddf-on-x jar's location(required) and press [ENTER]:

```


### Running the tests in sbt console 

In this case , the lib directory contains ddf-on-x jars and any changes of java Options are made 
in build.sbt.

```
$ sbt

//to run all the tests
sbt> test
```
### Reports:
To open the Html Reports of the tests generated, open index.html from target/test-reports directory in a web browser.
Clicking on the DDFSpec suite, will show the test reports.

### Note:
* For spark implementation, before running tests, you may need to execute the following to remove files:
  rm -r "./metastore_db/"
  rm -r "/tmp/hive/"
* Make sure that there are no duplicate jars present when adding jars to `lib` directory or when giving the jar 
  directory in the shell script. Do not include scalatest and junit jars . Do not remove the asm-all-4.0.jar from the lib directory as it
  contains some essential classes required for the scalatest to generate a html report.