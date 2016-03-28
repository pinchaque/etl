require 'etl/queue/payload'
require 'fileutils'
require 'tempfile'

module ETL::Queue

  # Simple file-based queue system that's ok for use in testing environment
  # This does not have good error checking as it's a bit hacky. 
  class File < Base    
    def initialize(params = {})
      @fname = params.fetch(:path, Tempfile.new("ETL_Queue_File").path)
    end
    
    def to_s
      "#{self.class.name}<#{@fname}>"
    end
    
    def enqueue(payload)
      ::File.open(@fname, "a") do |f|
        f.puts(payload.encode + "\n")
      end
    end

    # Removes all jobs from the queue
    def purge
      ::FileUtils.rm(@fname)
    end
    
    def message_count
      if ::File.exist?(@fname)
        %x{wc -l #{@fname}}.split.first.to_i
      else
        0
      end
    end

    # Process every line in our file on each iteration of the thread
    def process_async
      ::Thread.new do
        while true
          if ::File.exist?(@fname)
            tmp = ::Tempfile.new("ETL::Queue::File")
            # move to a temp file for processing in case any other lines
            # get added. This should prevent any race conditions as long
            # as the dirs are on the same filesystem
            ::FileUtils.mv(@fname, tmp.path)
            # Ugly - if we fail while reading then we lose queue items
            ::File.open(tmp.path, "r") do |f|
              while line = f.gets
                payload = ETL::Queue::Payload.decode(line)
                yield nil, payload
              end
            end
            tmp.unlink
          else
            # nothing in the queue - pause and then try again
            sleep(5) 
          end
        end
      end
    end
    
    def ack(msg_info)
      # do nothing
    end
  end
end
