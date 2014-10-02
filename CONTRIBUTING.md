<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [How to Contribute](#how-to-contribute)
  - [Workflow](#workflow)
  - [Testing](#testing)
  - [Style](#style)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# How to Contribute

We accept patches!

## Workflow

To start working on a feature or fix (on Github):

1.  Fork ZKTraffic
2.  Make a feature branch
3.  Work on your feature or bugfix
4.  Write a test for your change
5.  From your branch, make a pull request against twitter/zktraffic/master

## Testing

ZKTraffic uses nose for tests. All tests go in zktraffic/tests.

To run the tests:

```
$ nosetests -v zktraffic/tests
```

## Style

Make sure your code is PEP8 compliant and workes with Py2.7 and Py3K. Although, currently
Py3K is unsupported because of scapy, this should be fixed soon.
