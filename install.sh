#! /bin/bash

for i in $*
 do
  case $i in
    3) Python=/opt/rh/rh-python34/root/usr/bin/python ;;
    2) Python=/opt/rh/python27/root/usr/bin/python ;;
    0) Python=python ;;
    4) Python=python3.4 ;;
    *) echo "Need a python version: 3 for python 3, 2 for python 2 or 0 to use the system default"; exit 1 ;;
  esac
done

echo "Installing Python module for $Python"
$Python setup.py install
