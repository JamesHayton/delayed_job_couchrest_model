# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name              = 'delayed_job_couchrest_model'
  s.summary           = "CouchRest Model Backend For Delayed Job"
  s.version           = '0.0.1'
  s.authors           = ['James Hayton']
  s.date              = Date.today.to_s
  s.email             = ['jamesbhayton@gmail.com']
  s.extra_rdoc_files  = ["LICENSE", "README.md"]
  s.files             = Dir.glob("{lib,spec}/**/*") + %w[LICENSE README.md]
  s.homepage          = 'http://github.com/JamesHayton/delayed_job_couchrest_model'
  s.rdoc_options      = ['--charset=UTF-8']
  s.require_paths     = ['lib']
  #s.test_files        = Dir.glob('spec/**/*')

  s.add_runtime_dependency      'couchrest_model', '>= 0.11.0'
  s.add_runtime_dependency      'delayed_job',  '~> 3.0.0'
  s.add_runtime_dependency      'tzinfo',       '~> 0.3.31'
  #s.add_development_dependency  'rspec',        '>= 2.0'
end

