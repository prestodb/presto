#!/bin/sh -eu

VERSION=$1
TARGET=docs/$VERSION
CURRENT=docs/current

REPOSITORY=$HOME/.m2/repository
GROUP=com.facebook.presto
ARTIFACT=presto-docs

GROUPDIR=$(echo $GROUP | tr . /)

CENTRAL=central::default::https://repo1.maven.apache.org/maven2

if [ -e $TARGET ]
then
   echo "already exists: $TARGET"
   exit 100
fi

mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:get \
  -Dartifact=$GROUP:$ARTIFACT:$VERSION:zip -DremoteRepositories=$CENTRAL

unzip $REPOSITORY/$GROUPDIR/$ARTIFACT/$VERSION/$ARTIFACT-$VERSION.zip

mv html $TARGET

ln -sfh $VERSION $CURRENT

git add $TARGET $CURRENT

git commit -m "Add $VERSION docs"
