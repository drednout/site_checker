Some great things to do in this project: 
* Add HTTP API for managing entities in site_checker DB
* Add support for deleting entities from DB  

* Implement dynamic partition management
* Add monitoring & alerting(e.g. Prometheus)
* Write some UI for showing and analyzing check results
* Scale site_checker both vertically and horizontally, concept may look like:

```
a) Run N http checkers and db writers

b) Each checker selects & run only some checks from DB, based on some
hash function, e.g. % or jump consitent hash

c) Each checker writes to separate partition in Kafka

d) Run on db writer for each partition in kafka

e) Run all processes under supervisord, generate configs and run processes
on different machines using tools like fabric or ansible
```
