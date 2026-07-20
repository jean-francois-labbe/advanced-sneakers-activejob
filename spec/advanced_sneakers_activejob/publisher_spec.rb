# frozen_string_literal: true

require 'logger'

describe AdvancedSneakersActiveJob::Publisher do
  let(:delivery_exchange_name) { AdvancedSneakersActiveJob::LeveledDelayedPublisher::DELIVERY_EXCHANGE }

  describe '#declare_republish_queue_binding (Mandatory override)' do
    # Bypass BunnyPublisher::Base#initialize so unit tests don't need a broker.
    let(:publisher) do
      p = described_class.allocate
      allow(p).to receive(:logger).and_return(Logger.new(IO::NULL))
      p
    end

    # The Mandatory#setup_queue_for_republish declares the republish queue on the
    # publish channel, then calls declare_republish_queue_binding(queue). The
    # queue exposes its #channel and #name, and #bind to bind to an exchange.
    let(:queue_channel) { instance_double('Bunny::Channel', topic: delivery_exchange) }
    let(:delivery_exchange) { instance_double('Bunny::Exchange') }
    let(:queue) do
      instance_double('Bunny::Queue', name: 'orders', channel: queue_channel, bind: nil)
    end

    before do
      # super() chains into Mandatory#declare_republish_queue_binding, which binds
      # the queue to the publish exchange (activejob). Stub the super call so the
      # unit test isolates OUR added delivery-exchange bind; the activejob bind is
      # exercised by the real-broker test below.
      allow_any_instance_of(BunnyPublisher::Mandatory).to receive(:declare_republish_queue_binding)
    end

    it 'chains super (preserving the activejob bind) before adding the delivery bind' do
      expect_any_instance_of(BunnyPublisher::Mandatory).to receive(:declare_republish_queue_binding).with(queue)

      publisher.declare_republish_queue_binding(queue)
    end

    it 'idempotently declares delay.delivery.x as a durable topic exchange' do
      publisher.declare_republish_queue_binding(queue)

      expect(queue_channel).to have_received(:topic).with(delivery_exchange_name, durable: true)
    end

    it 'binds the queue to delay.delivery.x with a "#.<queue_name>" routing key' do
      publisher.declare_republish_queue_binding(queue)

      expect(queue).to have_received(:bind).with(delivery_exchange, routing_key: '#.orders')
    end

    context 'when the delivery-exchange bind fails' do
      let(:real_logger) { instance_double(Logger) }

      before do
        allow(publisher).to receive(:logger).and_return(real_logger)
        allow(real_logger).to receive(:error)
        # Model a real-ish broker refusal (e.g. 406 PRECONDITION_FAILED closing
        # the channel) surfacing from the bind.
        allow(queue).to receive(:bind).and_raise(
          Bunny::PreconditionFailed.new('PRECONDITION_FAILED - inequivalent arg', queue_channel, false)
        )
      end

      it 'does not break the republish flow (swallows the error)' do
        expect { publisher.declare_republish_queue_binding(queue) }.not_to raise_error
      end

      it 'logs the failure at error level naming the queue and exchange' do
        publisher.declare_republish_queue_binding(queue)

        expect(real_logger).to have_received(:error) do |&block|
          msg = block.call
          expect(msg).to include('orders')
          expect(msg).to include(delivery_exchange_name)
        end
      end

      it 'still performed the activejob bind via super' do
        expect_any_instance_of(BunnyPublisher::Mandatory).to receive(:declare_republish_queue_binding).with(queue)

        publisher.declare_republish_queue_binding(queue)
      end
    end
  end

  describe 'unrouted immediate publish auto-binds both exchanges', :rabbitmq do
    let(:connection) { Bunny.new(ENV.fetch('RABBITMQ_URL')).start }
    let(:logger) { Logger.new(IO::NULL) }

    let(:publisher) do
      described_class.new(
        exchange: 'activejob',
        exchange_options: { type: 'direct', durable: true },
        connection: connection
      )
    end

    before do
      allow(publisher).to receive(:logger).and_return(logger)
      allow(AdvancedSneakersActiveJob.config).to receive(:handle_unrouted_messages).and_return(true)

      # The delivery exchange must exist as a durable topic for the extra bind to
      # succeed; declare it up front (idempotent with the override's own declare).
      setup = connection.create_channel
      setup.topic(delivery_exchange_name, durable: true)
      setup.close
    end

    after { connection.close }

    it 'auto-creates the queue bound to BOTH activejob and delay.delivery.x' do
      # No queue named "ghost_orders" exists, so this immediate publish is
      # unrouted; Mandatory catches the return and republishes after declaring
      # the queue + bindings. The republish happens on a background thread.
      publisher.publish('payload', routing_key: 'ghost_orders')

      wait_for do
        rabbitmq_bindings(queue: 'ghost_orders', exchange: 'activejob').any? &&
          rabbitmq_bindings(queue: 'ghost_orders', exchange: delivery_exchange_name).any?
      end

      activejob_keys = rabbitmq_bindings(queue: 'ghost_orders', exchange: 'activejob').map(&:routing_key)
      delivery_keys = rabbitmq_bindings(queue: 'ghost_orders', exchange: delivery_exchange_name).map(&:routing_key)

      expect(activejob_keys).to include('ghost_orders')
      expect(delivery_keys).to include('#.ghost_orders')
    end

    # Poll the broker until the async republish thread has done its work, or fail.
    def wait_for(timeout: 5)
      deadline = Time.now + timeout
      loop do
        return if yield
        raise "condition not met within #{timeout}s" if Time.now > deadline

        sleep 0.05
      end
    rescue Bunny::NotFound
      # queue/bindings not there yet; keep polling until the deadline.
      raise if Time.now > deadline

      sleep 0.05
      retry
    end
  end
end
