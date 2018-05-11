#!/bin/bash

rm -f unique.txt
rm -f higher_chance.txt

awk -F, '{print $1}' output.csv | sort -u > unique.txt
grep -P '^[a-zA-z]{4,}\d+$' unique.txt > higher_chance.txt # aqingxin6688
grep -P '^[1-5]\d{9,11}$' unique.txt > higher_chance.txt # 101422288494
grep -P '^[qQ]{1,2}\d+$' unique.txt > higher_chance.txt # qq975555711
