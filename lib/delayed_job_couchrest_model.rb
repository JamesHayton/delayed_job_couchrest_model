# encoding: utf-8
require "couchrest_model"
require "delayed_job"
require "delayed/serialization/couchrest_model"
require "delayed/backend/couchrest_model"

Delayed::Worker.backend = :couchrest_model