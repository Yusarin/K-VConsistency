#!/usr/bin/env bash
gradle jar
if [ $(uname) = "Darwin" ]; then
    osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/Key-Val-Consistency.jar Process.MasterUp 0 Configuration1\""
else
    gnome-terminal --tab -x zsh -c "java -cp build/libs/Key-Val-Consistency.jar Process.MasterUp 0 TotalConfiguration"
fi
for id in $(seq 1 $1)
do
echo "$id"
if [ $(uname) = "Darwin" ]; then
    if [ -n "$2" ]; then
        osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/Key-Val-Consistency.jar Process.LinearizableDemo $id Configuration1 $2/$id\""
    else
        osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/Key-Val-Consistency.jar Process.LinearizableDemo $id Configuration1\""
    fi
else
    if [ -n "$2" ]; then
        gnome-terminal --tab -x zsh -c "java -cp build/libs/Key-Val-Consistency.jar Process.LinearizableDemo $id Configuration1 $2/$id"
    else
        gnome-terminal --tab -x zsh -c "java -cp build/libs/Key-Val-Consistency.jar Process.LinearizableDemo $id Configuration1"
    fi
fi
done
