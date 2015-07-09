module Delayed
  class Worker
        
    def run(job)
      begin
        Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }
        job.status       = "Completed"
        job.completed_at = Time.now 
        job.run_time    += job.completed_at-job.locked_at
        job.run_at       = nil
        job.unlock
        job.clear_failed
        job.save
        return true  # Job Succeeded
      rescue DeserializationError => error
        job.last_error = "{#{error.message}\n#{error.backtrace.join("\n")}"
        failed(job)
      rescue Exception => error
        self.class.lifecycle.run_callbacks(:error, self, job) { handle_failed_job(job, error) }
        return false  # Job Failed
      end
    end
    
    def reschedule(job, time = nil)
      if (job.attempts + 1) <= max_attempts(job)
        time        ||= job.reschedule_at
        job.run_time += (Time.now.utc - job.locked_at)
        job.failed_at = Time.now
        job.run_at    = time
        job.status    = "Failed - Rescheduled"
        job.unlock
        job.save!
      else
        job.run_time += (Time.now.utc - job.locked_at)
        job.failed_at = Time.now
        job.run_at    = nil
        job.status    = "Failed - Attempts Exceeded"
        job.unlock
        job.save!
      end
    end
    
  end  
  module Backend
    module CouchrestModel
      class Job < CouchRest::Model::Base
        include Delayed::Backend::Base
        
        use_database "delayed_jobs"

        property :priority,     Integer, default: 0
        property :attempts,     Integer, default: 0
        property :max_attempts, Integer, default: 10
        property :handler
        property :run_at,       Time
        property :locked_at,    Time
        property :locked_by
        property :failed_at,    Time
        property :last_error
        property :completed_at, Time
        property :run_time,     Float,  default: 0.0
        property :trigger
        property :tags
        property :queue
        property :status,               default: "Pending"  
        
        timestamps!
        
        design do
          view :by_failed_at_locked_by_and_run_at,
            map:
              "function(doc) {
                if ((doc.type == 'Delayed::Backend::CouchrestModel::Job') && (doc.status != 'Completed') && (doc.status != 'Failed - Attempts Exceeded') && (doc.attempts < doc.max_attempts)) {
                  emit([null, doc.locked_by || null, doc.run_at || null], null);
                } 
              }"
            view :by_failed_at_locked_at_and_run_at,
              map:
                "function(doc) {
                  if ((doc.type == 'Delayed::Backend::CouchrestModel::Job') && (doc.status != 'Completed') && (doc.status != 'Failed - Attempts Exceeded') && (doc.attempts < doc.max_attempts)) {
                    emit([null, doc.locked_at || null, doc.run_at || null], null);
                  } 
                }"
        end # Design

        before_save :set_default_run_at, :round_run_time
        
        # payload_object method is used to access everything that was passed to the job
  
        
        def method_missing(method_sym, *arguments, &block)
          if self["#{method_sym.to_s}"] != nil
            self["#{method_sym.to_s}"]
          else
            super
          end
        end

        def self.db_time_now
          Time.now.utc
        end
        
        def self.reserve(worker, max_run_time = Worker.max_run_time)
          find_available(worker, worker.read_ahead, max_run_time).detect do |job|
            job.lock_exclusively!(max_run_time, worker.name)
          end
        end
        
        def self.find_available(worker, limit = 5, max_run_time = ::Delayed::Worker.max_run_time)
          worker_name = worker.name
          ready       = ready_jobs
          mine        = my_jobs(worker_name) 
          expire      = expired_jobs(max_run_time)
          jobs_array  = []
                  
          [ready, mine, expire].each do |view_results|
            view_results.each do |result|
              jobs_array << result
            end  
          end
          
          jobs_array.sort_by { |j| j.priority }
          jobs_array   = jobs_array.find_all { |j| j.priority >= Worker.min_priority } if Worker.min_priority
          jobs_array   = jobs_array.find_all { |j| j.priority <= Worker.max_priority } if Worker.max_priority
          jobs_array   = jobs_array.find_all { |j| Worker.queues.include? j.queue }    if Worker.queues.any?
          jobs_array[0..limit-1]
        end

        def lock_exclusively!(max_run_time, worker = worker_name)
          return false if locked_by_other?(worker) and not expired?(max_run_time)
          case
          when locked_by_me?(worker)
            self.locked_at = self.class.db_time_now
          when unlocked? || (locked_by_other?(worker) && expired?(max_run_time))
            self.locked_at, self.locked_by, self.status, self.attempts = self.class.db_time_now, worker, "Working", self.attempts += 1
          end
          save
        rescue RestClient::Conflict
          false
        end
        
        def locked?
          locked_by.present? || locked_at.present?
        end
        
        def completed?
          status === "Completed"
        end
        
        def in_progress?
          status === "Working"
        end
        
        def failed?
          failed_at.present? || last_error.present?
        end
        
        def runable?
          status != "Completed"
        end
        
        def clear_failed
          self.failed_at, self.last_error = nil
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
        
        def set_default_run_at
          self.run_at ||= self.class.db_time_now if self.new?
        end
        
        def round_run_time
          self.run_time = run_time.round(2)
        end
        
        # end private methods
        
      end # Job Class
    end # CouchrestModel
  end # Backend
end # Delayed
