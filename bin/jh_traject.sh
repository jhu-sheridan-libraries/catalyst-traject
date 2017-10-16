#!/usr/bin/env bash

# A wrapper for traject that uses chruby to make sure jruby
# is being used before calling traject, and then calls
# traject with bundle exec from within our traject project
# dir. 

# Make sure /usr/local/bin is in PATH for chruby-exec,
# which it's not ordinarily in a cronjob. 
if [[ ":$PATH:" != *":/usr/local/bin:"* ]]
then
  export PATH=$PATH:/usr/local/bin
fi
# chruby needs SHELL set, which it won't be from a crontab
export SHELL=/bin/bash


# Go one dir up from where this script is located,
# to find traject dir to bundle exec in, to find proper Gemfile. 
# phew. 
traject_dir=$(cd `dirname "${BASH_SOURCE[0]}"`/../ && pwd)

# do we need to use chruby to switch to jruby?
if [[ "$(ruby -v)" == *jruby* ]]
then
  ruby_picker="" # nothing needed "
else
  ruby_picker="chruby-exec jruby --"
fi

cmd="BUNDLE_GEMFILE=$traject_dir/Gemfile $ruby_picker bundle exec traject $@"

echo $cmd
eval $cmd