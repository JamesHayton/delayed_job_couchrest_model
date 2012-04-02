# CouchRest Model Backend For Delayed Job

## Installation

Add the gems to your Gemfile:

    gem 'delayed_job'
    gem 'delayed_job_couchrest_model', :git => "git://github.com/JamesHayton/delayed_job_couchrest_model.git"

That's it. Use [delayed_job as normal](http://github.com/collectiveidea/delayed_job).

Please note: While this currently works, there are changes/improvements that need to be made.  I plan on making them before my project that is using this launches, but until then be careful. There is no support for multiple queues yet and there are some optimizations that could/should be made.

## From

I mostly copied this from someone who had made a CouchDB Delayed Job Backend prior to version 3.0.  I can't find him or remember his name, but I had this one file stashed on my hard drive.  I would like to give the dude credit, but I can't remember his name/github account.

I just changed a few things here and there to support the new way CouchRest Model does things. 