module Delayed
  class Worker
    def run(job)
          runtime =  Benchmark.realtime do
            Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }
            job.status = "Completed"
            job.save
          end
          say "#{job.name} completed after %.4f" % runtime
          return true  # did work
        rescue DeserializationError => error
          job.last_error = "{#{error.message}\n#{error.backtrace.join("\n")}"
          failed(job)
        rescue Exception => error
          self.class.lifecycle.run_callbacks(:error, self, job){ handle_failed_job(job, error) }
          return false  # work failed
        end
  end  
  module Backend
    module CouchrestModel
      class Job < CouchRest::Model::Base
        include Delayed::Backend::Base
        
        use_database ::CouchRest::Server.new.database("delayed_jobs")

        property :priority,   Integer, :default => 0
        property :attempts,   Integer, :default => 0
        property :handler
        property :run_at,     Time
        property :locked_at,  Time
        property :locked_by
        property :failed_at,  Time
        property :last_error
        property :queue
        property :status,     :default => "Pending"  
        timestamps!
        
        design do
          view :by_failed_at_locked_by_and_run_at,
            :map =>
              "function(doc) {
                if ((doc.type == 'Delayed::Backend::CouchrestModel::Job') && (doc.status != 'Completed')) {
                  emit([doc.failed_at || null, doc.locked_by || null, doc.run_at || null], null);
                } 
              }"
            view :by_failed_at_locked_at_and_run_at,
              :map =>
                "function(doc) {
                  if ((doc.type == 'Delayed::Backend::CouchrestModel::Job') && (doc.status != 'Completed')) {
                    emit([doc.failed_at || null, doc.locked_at || null, doc.run_at || null], null);
                  } 
                }"
        end # Design

        before_save :set_default_run_at

        def self.db_time_now
          Time.now.utc
        end
        
        def self.find_available(worker_name, limit = 5, max_run_time = ::Delayed::Worker.max_run_time)
                  ready  = ready_jobs
                  mine   = my_jobs(worker_name) 
                  expire = expired_jobs(max_run_time)
                  jobs_array = []
                  [ready, mine, expire].each do |view_results|
                    view_results.each do |result|
                      jobs_array << result
                    end  
                  end
                  jobs_array[0..limit-1].sort_by { |j| j.priority }  
                  #jobs   = (ready + mine + expire)
                  jobs_array   = jobs.find_all { |j| j.priority >= Worker.min_priority } if Worker.min_priority
                  jobs_array   = jobs.find_all { |j| j.priority <= Worker.max_priority } if Worker.max_priority
                  jobs_array
                end

        def lock_exclusively!(max_run_time, worker = worker_name)
          return false if locked_by_other?(worker) and not expired?(max_run_time)
          case
          when locked_by_me?(worker)
            self.locked_at = self.class.db_time_now
          when (unlocked? or (locked_by_other?(worker) and expired?(max_run_time)))
            self.locked_at, self.locked_by, self.status = self.class.db_time_now, worker, "Working"
          end
          save
        rescue RestClient::Conflict
          false
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          jobs = my_jobs(worker_name) 
          jobs.each { |j| j.locked_by, j.locked_at = nil, nil; }
          database.bulk_save(jobs)
        end
        
        def self.delete_all
          database.bulk_save(all.each { |job| job["_deleted"] = true })
        end  

        def reload(*args)
          reset
          super
        end
        
        private
      
        def self.ready_jobs
          by_failed_at_locked_by_and_run_at.startkey([nil, nil]).endkey([nil, nil, db_time_now])
        end
        
        def self.my_jobs(worker_name)
          by_failed_at_locked_by_and_run_at.startkey([nil, worker_name]).endkey([nil, worker_name, {}])
        end  
        
        def self.expired_jobs(max_run_time)
          by_failed_at_locked_at_and_run_at.startkey([nil, '0']).endkey([nil, db_time_now - max_run_time, db_time_now])
        end
        
        def unlocked?
          locked_by.nil?
        end
        
        def expired?(time)
          locked_at < self.class.db_time_now - time
        end
        
        def locked_by_me?(worker)
          not locked_by.nil? and locked_by == worker
        end
        
        def locked_by_other?(worker)
          not locked_by.nil? and locked_by != worker
        end
        
        # end private methods
        
      end # Job Class
    end # CouchrestModel
  end # Backend
end # Delayed
