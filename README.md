# ddf-test
Test suite for DDF

### Running test suite

1. Add ddf-on-x implementation jar to `lib` directory. Make sure that there are no duplicate jars present. Remove
scalatest and junit jars if added from ddf-on-x. Do not remove the asm-all-4.0.jar from the lib directory as it
contains some essential classes required for the scalatest to generate a html report.

2. Execute the tests in console

```
$ sbt

//to run all the tests
sbt> test

To open the Html Reports of the tests generated, open index.html from target/test-reports directory in a web browser.
Clicking on the DDFSpec suite, will show the test reports.

```