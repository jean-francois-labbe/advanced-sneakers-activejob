# frozen_string_literal: true

module AdvancedSneakersActiveJob
  class Publisher < ::BunnyPublisher::Base
    include ::BunnyPublisher::Mandatory

    before_publish :log_message

    delegate :logger, to: :'::ActiveJob::Base'

    delegate :handle_unrouted_messages,
             to: :'AdvancedSneakersActiveJob.config',
             prefix: :config

    # OVERRIDE of BunnyPublisher::Mandatory#declare_republish_queue_binding.
    #
    # The parent hook (bunny-publisher 0.2.0 mandatory.rb) is called with the
    # freshly-declared republish queue and binds it to the publish exchange
    # (`activejob`) ONLY. Queues minted this way are reachable for immediate
    # jobs but NOT for leveled delayed jobs, violating the leveled invariant
    # "reachable for immediate => reachable for delayed".
    #
    # We chain `super` (preserving the activejob bind) and then idempotently
    # declare delay.delivery.x (topic, durable) and bind this queue to it with
    # routing key "#.<queue_name>" — matching LeveledDelayedPublisher's
    # destination binding contract, so an auto-created queue is immediately
    # reachable for leveled delayed delivery too.
    #
    # The queue's channel is the same publish channel the parent uses
    # (declare_republish_queue calls `channel.queue(...)`), so we declare and
    # bind the delivery exchange on `queue.channel`.
    #
    # Failure posture (same as BEP-9889/9890): the extra bind must never fail
    # the republish. Rescue any broker error, log at error level (naming the
    # queue + exchange), and continue — the activejob bind from `super` already
    # makes the message deliverable for immediate jobs.
    def declare_republish_queue_binding(queue)
      super

      bind_queue_to_delivery_exchange(queue)
    end

    private

    def bind_queue_to_delivery_exchange(queue)
      delivery_exchange_name = ::AdvancedSneakersActiveJob::LeveledDelayedPublisher::DELIVERY_EXCHANGE

      delivery_exchange = queue.channel.topic(delivery_exchange_name, durable: true)
      queue.bind(delivery_exchange, routing_key: "#.#{queue.name}")
    rescue StandardError => e
      logger.error do
        "AdvancedSneakersActiveJob::Publisher: failed to bind auto-created queue [#{queue.name}] " \
        "to delivery exchange [#{::AdvancedSneakersActiveJob::LeveledDelayedPublisher::DELIVERY_EXCHANGE}] " \
        "(#{e.class}: #{e.message}); queue remains reachable for immediate jobs but NOT for leveled " \
        'delayed jobs until the binding is created.'
      end
    end

    def log_message
      logger.debug do
        "Publishing <#{message}> to [#{@exchange_name}] with routing_key [#{message_options[:routing_key]}]"
      end
    end

    def on_message_return(return_info, properties, message)
      if config_handle_unrouted_messages
        super
      else
        logger.warn do
          "Message is not routed! #{{ message: message, return_info: return_info, properties: properties }}"
        end
      end
    end
  end
end
