#!/bin/bash

wget -nc http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz
tar xvf profiledata_06-May-2005.tar.gz
mv profiledata_06-May-2005/* .
rm -r profiledata_06-May-2005 profiledata_06-May-2005.tar.gz