#!/bin/bash

db_set() {
    echo "$1,$2" >> database
}

db_get() {
    grep "^$1," database | sed -e "s/^$1,//" | tail -n 1
}

db_set 42 '{"a": 12}'
db_set 45 'dafsdfsd'
db_set 48 '1'
db_get 42
# db_get 43
db_get 48

