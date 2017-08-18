require 'spec_helper'

RSpec.describe ETL::Metrics do
  let(:series) { 'weather' }
  let(:described_instance) do
    described_class.new({ series: series })
  end
  let(:tags) { { humidity: 67.1 } }

  describe '#point' do
    let(:values) { { temp: 14.2 } }
    let(:now) { Time.now }

    it 'writes a point' do
      expect(described_instance).to receive(:publish)
        .with(
          hash_including(
            series: series,
            values: values,
            tags: tags,
            time: anything,
            type: anything
          )
        )
      described_instance.point(values, tags: tags)
    end

    it 'writes a point with specific time' do
      expect(described_instance).to receive(:publish)
        .with(
          hash_including(
            series: series,
            values: values,
            tags: tags,
            time: now,
            type: anything
          )
        )
      described_instance.point(values, tags: tags, time: now)
    end
  end

  describe '#time' do
    let(:thing) { double('timed thing') }

    it 'times a block' do
      expect(thing).to receive(:run)
      expect(described_instance).to receive(:publish)
        .with(
          hash_including(
            series: series,
            values: { duration: anything },
            tags: tags,
            time: anything,
            type: :timer
          )
        )
      described_instance.time(tags: tags) do
        thing.run
      end
    end    
  end
end
