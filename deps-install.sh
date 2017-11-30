#!/bin/bash
# Copyright 2018, Imperial College London.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.

sudo apt-get update -y
sudo add-apt-repository ppa:webupd8team/java -y

sudo apt-get install curl

sudo apt-get install oracle-java8-installer
sudo apt-get install maven

# leiningen - Clojure
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod +x lein
sudo mv lein /usr/local/bin