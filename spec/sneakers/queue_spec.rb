# frozen_string_literal: true

describe 'Sneakers::Queue patch' do
  describe '#subscribe', :rabbitmq do
    let(:connection) { Bunny.new(ENV.fetch('RABBITMQ_URL')).start }
    let(:opts) { AdvancedSneakersActiveJob.config.sneakers.merge(connection: connection) }
    let(:worker) { double('worker', opts: { handler: AdvancedSneakersActiveJob::Handler }) }
    let(:queue) { Sneakers::Queue.new('custom', opts) }

    after { connection.close }

    def routing_keys(queue:, exchange:)
      rabbitmq_bindings(queue: queue, exchange: exchange).map(&:routing_key)
    end

    it 'binds the queue to both the ActiveJob exchange and delay.delivery.x' do
      queue.subscribe(worker)

      expect(routing_keys(queue: 'custom', exchange: 'activejob')).to eq(['custom'])
      expect(routing_keys(queue: 'custom', exchange: 'delay.delivery.x')).to eq(['#.custom'])
    end

    context 'with a custom handler' do
      let(:worker) { double('worker', opts: { handler: Sneakers::Handlers::Oneshot }) }

      it 'still binds to delay.delivery.x' do
        queue.subscribe(worker)

        expect(routing_keys(queue: 'custom', exchange: 'delay.delivery.x')).to eq(['#.custom'])
      end
    end

    context 'with a non-ActiveJob exchange' do
      let(:opts) { AdvancedSneakersActiveJob.config.sneakers.merge(connection: connection, exchange: 'sneakers') }

      it 'does not bind to delay.delivery.x' do
        connection.create_channel.topic('delay.delivery.x', durable: true) # so the bindings query does not 404

        queue.subscribe(worker)

        expect(routing_keys(queue: 'custom', exchange: 'sneakers')).to eq(['custom'])
        expect(routing_keys(queue: 'custom', exchange: 'delay.delivery.x')).to eq([])
      end
    end

    context 'when the bind fails' do
      let(:log) { StringIO.new }

      around do |example|
        original_logger = Sneakers.logger
        Sneakers.logger = Logger.new(log)
        example.run
      ensure
        Sneakers.logger = original_logger
      end

      before do
        allow_any_instance_of(Bunny::Channel).to receive(:topic).and_raise('the broker said no')
      end

      it 'logs an error and consumes anyway' do
        messages = Queue.new
        allow(worker).to receive(:do_work) { |_, _, msg, _| messages.push(msg) }

        expect { queue.subscribe(worker) }.not_to raise_error
        expect(log.string).to include('Failed to bind queue [custom] to exchange [delay.delivery.x]: RuntimeError: the broker said no')

        connection.create_channel.direct('activejob', durable: true).publish('payload', routing_key: 'custom')

        expect(messages.pop(timeout: 5)).to eq('payload')
      end
    end
  end

  describe AdvancedSneakersActiveJob::SneakersQueuePatch, '.cross_matching_names' do
    it 'reports known queue names that dot-suffix the new name (and vice versa)' do
      expect(described_class.cross_matching_names('crossmatch.a.b')).to eq([])
      expect(described_class.cross_matching_names('b')).to eq(['crossmatch.a.b'])
      expect(described_class.cross_matching_names('a.b')).to eq(['crossmatch.a.b', 'b'])
      expect(described_class.cross_matching_names('unrelated')).to eq([])
    end
  end
end
