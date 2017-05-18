require 'json'

module ETL
  # For reporting job & data metrics
  # TODO:
  # * report batch size or row count
  class Metrics
    attr_reader :series

    def initialize(params = {})
      @series = params.fetch(:series)
    end

    def point(values, tags: {}, time: Time.now, type: :gauge)
      p = {
        series: @series,
        time: time,
        values: values,
        tags: tags,
        type: type
      }
      publish(p)
    end

    def time(tags: {}, &block)
      start_time = Time.now
      yield tags
      end_time = Time.now
      point({ duration: end_time - start_time }, tags: tags, time: end_time, type: :timer)
    end

    protected

    def publish(point)
      puts(point.to_json)
    end
  end
end
