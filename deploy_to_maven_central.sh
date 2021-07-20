#!/bin/sh
if [ $# != 1 ]; then
   echo "Please provide gpg passphrase"
   exit 1;
fi

mvn clean javadoc:jar source:jar deploy -Dgpg.passphrase=$1 -DperformRelease=true
