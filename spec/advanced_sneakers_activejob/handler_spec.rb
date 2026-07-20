# frozen_string_literal: true

require 'logger'
require 'active_job' # #error routes through the adapter, which reads ::ActiveJob::Base.logger
require 'active_job/queue_adapters' # defines ActiveJob::QueueAdapters.lookup, touched lazily by ActiveJob::Base.logger

describe AdvancedSneakersActiveJob::Handler do
  # Oneshot#initialize(channel, queue, opts) — only the channel matters for
  # #acknowledge; the queue/opts are unused here.
  let(:channel) { instance_double('Bunny::Channel', acknowledge: nil) }
  let(:handler) { described_class.new(channel, 'custom', {}) }

  # A Bunny consumer whose queue reports its name — the handler reads this to
  # build the x-death row and to compute the retry delay.
  let(:queue) { instance_double('Bunny::Queue', name: 'custom') }
  let(:consumer) { instance_double('Bunny::Consumer', queue: queue) }
  let(:delivery_info) do
    instance_double(
      'Bunny::DeliveryInfo',
      routing_key: 'custom',
      exchange: 'activejob',
      delivery_tag: 'tag-1',
      consumer: consumer
    )
  end

  # AMQP properties are passed as a Bunny::MessageProperties-like object; the
  # handler only calls #to_h on it. Start with empty headers so the handler
  # populates x-death and the computed retry delay itself.
  let(:properties) { instance_double('Bunny::MessageProperties', to_h: { headers: {} }) }

  let(:message) { 'serialized-job-payload' }
  let(:error) { StandardError.new('Some error message') }

  let(:legacy_publisher) { instance_double(AdvancedSneakersActiveJob::DelayedPublisher, publish: nil) }
  let(:leveled_publisher) { instance_double(AdvancedSneakersActiveJob::LeveledDelayedPublisher, publish: nil, max_delay: 1_048_575) }

  before do
    allow(AdvancedSneakersActiveJob).to receive(:delayed_publisher).and_return(legacy_publisher)
    allow(AdvancedSneakersActiveJob).to receive(:leveled_delayed_publisher).and_return(leveled_publisher)
    # The adapter's selection logic logs via ::ActiveJob::Base.logger; give it a
    # quiet real logger (assign rather than stub to avoid triggering ActiveJob's
    # autoload machinery under verify_partial_doubles).
    @previous_logger = ::ActiveJob::Base.logger
    ::ActiveJob::Base.logger = Logger.new(IO::NULL)
  end

  after { ::ActiveJob::Base.logger = @previous_logger }

  describe '#error routes the retry through the adapter selection logic' do
    context 'when config.delayed_delivery is :legacy (default)' do
      it 'republishes via the legacy delayed_publisher' do
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy

        handler.error(delivery_info, properties, message, error)

        expect(legacy_publisher).to have_received(:publish).with(message, hash_including(routing_key: 'custom'))
        expect(leveled_publisher).not_to have_received(:publish)
      ensure
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
      end

      it 'acknowledges the original message' do
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy

        handler.error(delivery_info, properties, message, error)

        expect(channel).to have_received(:acknowledge).with('tag-1', false)
      ensure
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
      end

      it 'passes the computed retry delay through in the headers' do
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
        # First failure -> retry_delay_proc.call(1) -> 3 seconds (default backoff).
        handler.error(delivery_info, properties, message, error)

        expect(legacy_publisher).to have_received(:publish) do |_msg, params|
          expect(params[:headers]['delay']).to eq(3)
        end
      ensure
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
      end
    end

    context 'when config.delayed_delivery is :leveled' do
      it 'republishes via the leveled_delayed_publisher' do
        AdvancedSneakersActiveJob.config.delayed_delivery = :leveled

        handler.error(delivery_info, properties, message, error)

        expect(leveled_publisher).to have_received(:publish).with(message, hash_including(routing_key: 'custom'))
        expect(legacy_publisher).not_to have_received(:publish)
      ensure
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
      end
    end

    context 'when config.delayed_delivery is a callable' do
      it 'evaluates the callable per republish' do
        calls = []
        AdvancedSneakersActiveJob.config.delayed_delivery = -> { calls << :called; :leveled }

        handler.error(delivery_info, properties, message, error)

        expect(calls).to eq([:called])
        expect(leveled_publisher).to have_received(:publish)
      ensure
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
      end
    end

    context 'when :leveled but the computed delay overflows max_delay' do
      it 'downgrades to the legacy publisher for that republish' do
        AdvancedSneakersActiveJob.config.delayed_delivery = :leveled
        allow(leveled_publisher).to receive(:max_delay).and_return(1) # force overflow (delay=3 > 1)

        handler.error(delivery_info, properties, message, error)

        expect(legacy_publisher).to have_received(:publish)
        expect(leveled_publisher).not_to have_received(:publish)
      ensure
        AdvancedSneakersActiveJob.config.delayed_delivery = :legacy
      end
    end
  end
end
