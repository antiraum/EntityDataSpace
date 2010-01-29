This is the implementation of a data space that stores entities consisting of ids and name/value attributes in a key/value store (Berkeley DB). It supports tree-like queries with wildcards and variables. Attributes can be mapped to alternative name/value pairs to allow flexibility in the query syntax.

The data_space directory contains the classes of the data space implementation. The test directory contains the unit tests for the implementation. The benchmark directory holds the code of the benchmarks executed for the evaluation.

The script dii.rb shows exemplary how the data space can be used.