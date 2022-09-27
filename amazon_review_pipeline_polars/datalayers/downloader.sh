#!/usr/bin/env bash

wget -P datalayers/landing http://deepyeti.ucsd.edu/jianmo/amazon/categoryFilesSmall/Toys_and_Games_5.json.gz
gzip -dk datalayers/landing/Toys_and_Games_5.json.gz
rm -rf datalayers/landing/Toys_and_Games_5.json.gz