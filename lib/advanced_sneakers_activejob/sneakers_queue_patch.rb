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
        @mutex.synchronize do
          matches = @known_names.select { |other| other.end_with?(".#{name}") || name.end_with?(".#{other}") }.to_a
          @known_names << name
          matches
        end
      end
    end

    # Eager init: subscribers run concurrently, lazy ||= would race.
    @mutex = Mutex.new
    @known_names = Set.new

    def subscribe(worker)
      super.tap { bind_to_delayed_delivery_exchange }
    end

    private

    def bind_to_delayed_delivery_exchange
      return unless opts[:exchange] == AdvancedSneakersActiveJob.config.sneakers[:exchange]

      delivery_exchange = LeveledDelayedPublisher::DELIVERY_EXCHANGE

      declare_and_bind(delivery_exchange)
      warn_on_cross_matching_names(delivery_exchange)
    rescue StandardError => e
      # Fail open: a consumer without the delay binding beats a crash-looping
      # fleet. Delayed jobs maturing for this queue are dropped until the
      # binding exists.
      log_bind_failure(delivery_exchange, e)
    end

    # Dedicated short-lived channel: broker-side declare/bind failures
    # (403 access_refused, 406 precondition_failed) close the channel that
    # issued them, which must never be the consumer's.
    def declare_and_bind(delivery_exchange)
      bind_channel = channel.connection.create_channel
      # Idempotent declare first: binding to a missing exchange 404s on
      # hosts that never declared the topology.
      bind_channel.topic(delivery_exchange, durable: true)

      # Matured messages carry routing key "<bits>.<original key>", so mirror
      # every immediate-path binding key (kicks binds routing_key || name),
      # plus the queue name — the default for fresh delayed publishes.
      [*(opts[:routing_key] || name), name].uniq.each do |key|
        bind_channel.queue_bind(name, delivery_exchange, routing_key: "#.#{key}")
      end
    ensure
      bind_channel.close if bind_channel&.open?
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
