# Rusty Pipes

A library that enables you to declaratively define your data sources and transformation pipelines, with an engine to
fetch data and run said pipeline

Please refer to the [examples](./examples) for a sense of how this library can be used.

For now, this library is relatively bare-bones. It contains one Loader (for CSV files) and two transformations (Filter
and Inner Join). 

### Contributions
Contributions are most welcome; the plan is to provide a wider variety of Loaders (e.g., a
Postgres-based loader) as well as many more Transformations. These should be relatively straightforward to implement: 
Loaders must implement the `Loader` trait and Transformations must implement the `Transformation` trait. That's it.

