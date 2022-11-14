#! /bin/bash

mongoimport --host mongodb --db mongodb --collection census --type json --file /mongo-seed/census.json