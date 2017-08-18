# mock class to avoid having to hit the database to test out all the different
# cases for scheduling
class JobRunRepositoryMock
  def has_pending?(job, batch)
    @has_pending
  end

  def was_successful?(job, batch)
    @was_successful
  end

  def set_has_pending(v)
    @has_pending = v
  end

  def set_was_successful(v)
    @was_successful = v
  end

end

ETL::Model::JobRunRepository.instance = JobRunRepositoryMock.new

class ScheduleBaseSpecJob < ETL::Job::Base
  register_job
end

RSpec.describe "schedule/base" do
  before :all do
    ::ETL::Model::JobRunRepository.instance = JobRunRepositoryMock.new
  end
  after:all do
    ::ETL::Model::JobRunRepository.instance = ETL::Model::JobRunRepository.new
  end

  let(:batch) { ETL::Batch.new({ day: "2015-03-31" }) }
  let(:job) { ScheduleBaseSpecJob.new(batch) }

  {
    always: [
      { pending: false, success: false, ready: true },
      { pending: false, success: true, ready: true },
      { pending: true, success: false, ready: true },
      { pending: true, success: true, ready: true },
    ],
    never: [
      { pending: false, success: false, ready: false },
      { pending: false, success: true, ready: false },
      { pending: true, success: false, ready: false },
      { pending: true, success: true, ready: false },
    ],
    continuous: [
      { pending: false, success: false, ready: true },
      { pending: false, success: true, ready: true },
      { pending: true, success: false, ready: false },
      { pending: true, success: true, ready: false },
    ],
    once: [
      { pending: false, success: false, ready: true },
      { pending: false, success: true, ready: false },
      { pending: true, success: false, ready: false },
      { pending: true, success: true, ready: false },
    ],
  }.each do |name, examples|
    klass = "ETL::Schedule::#{name.capitalize}"
    describe klass do
      examples.each do |h|
        it h do
          ETL::Model::JobRunRepository.instance.set_has_pending(h[:pending])
          ETL::Model::JobRunRepository.instance.set_was_successful(h[:success])
          sch = Object::const_get(klass).new(job, batch)
          expect(sch.ready?).to eq(h[:ready])
        end
      end
    end
  end
end
