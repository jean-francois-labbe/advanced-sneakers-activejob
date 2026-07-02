# frozen_string_literal: true

module AdvancedSneakersActiveJob
  # Prepended to Sneakers::Queue to restore the "reachable for immediate jobs
  # implies reachable for delayed jobs" invariant of the legacy delayed path.
  #
  # Matured messages leave the leveled delay topology via a broker-internal
  # dead-letter republish to LeveledDelayedPublisher::DELIVERY_EXCHANGE, so an
  # unbound destination queue drops them silently (no mandatory flag or
  # publisher callback can catch it). Binding at subscribe time covers every
  # consumer of the ActiveJob exchange, including host-defined worker classes
  # that bypass AdvancedSneakersActiveJob.define_consumer, and is independent
  # of the worker's handler.
  #
  # The bind is deliberately NOT gated on config.delayed_delivery: it is a
  # per-publish (potentially callable) setting, so its boot-time value proves
  # nothing. An extra durable binding is inert while the leveled path is
  # unused and makes flag flips safe in both directions.
  module SneakersQueuePatch
    class << self
      # Tracks bound queue names to warn about topic cross-matches:
      # "#.b" also matches deliveries destined for queue "a.b".
      def cross_matching_names(name)
        @mutex ||= Mutex.new
        @mutex.synchronize do
          @known_names ||= Set.new
          matches = @known_names.select { |other| other.end_with?(".#{name}") || name.end_with?(".#{other}") }.to_a
          @known_names << name
          matches
        end
      end
    end

    def subscribe(worker)
      super.tap { bind_to_delayed_delivery_exchange }
    end

    private

    def bind_to_delayed_delivery_exchange
      return unless opts[:exchange] == AdvancedSneakersActiveJob.config.sneakers[:exchange]

      delivery_exchange = LeveledDelayedPublisher::DELIVERY_EXCHANGE

      # Idempotent declare first: binding to a missing exchange 404s and
      # closes the consumer channel on hosts that never declared the topology.
      channel.topic(delivery_exchange, durable: true)
      channel.queue_bind(name, delivery_exchange, routing_key: "#.#{name}")

      warn_on_cross_matching_names(delivery_exchange)
    rescue StandardError => e
      # Fail open: a consumer without the delay binding beats a crash-looping
      # fleet. Delayed jobs maturing for this queue are dropped until the
      # binding exists.
      log_bind_failure(delivery_exchange, e)
    end

    def log_bind_failure(delivery_exchange, error)
      (Sneakers.logger || Logger.new($stderr)).error(
        "Failed to bind queue [#{name}] to exchange [#{delivery_exchange}]: #{error.class}: #{error.message}"
      )
    end

    def warn_on_cross_matching_names(delivery_exchange)
      SneakersQueuePatch.cross_matching_names(name).each do |other|
        Sneakers.logger&.warn "Queue [#{name}] binding [#.#{name}] on [#{delivery_exchange}] cross-matches queue [#{other}]"
      end
    end
  end
end

Sneakers::Queue.prepend(AdvancedSneakersActiveJob::SneakersQueuePatch)
