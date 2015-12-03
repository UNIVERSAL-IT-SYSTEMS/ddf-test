# ddf-test
Test suite for DDF

### Running test suite

1. Add ddf-on-x implementation jar to `lib` directory
2. Execute the tests in Scala console, using any of the following commands

```
$ sbt console

//to run all the tests
scala> new io.ddf.DDFSpec(<engineName>) execute 

For example,
scala> new io.ddf.DDFSpec("spark") execute

//to run all the tests for specified handler
scala> new io.ddf.DDFSpec(<engineName>) execute <handlerName> 

For example,
scala> new io.ddf.DDFSpec("spark") execute "Statistics" 

//to run a specific test for given handler
scala> new io.ddf.DDFSpec(<engineName>) execute <all or part of test name> 

For example, to run just the BinningHandler's test for equal Interval
scala> new io.ddf.DDFSpec("spark") execute "equal Interval" 

```